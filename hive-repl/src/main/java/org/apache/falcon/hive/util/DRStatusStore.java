/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.hive.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DRStatusStore {

    private static final Logger LOG = LoggerFactory.getLogger(DRStatusStore.class);

    public static final String BASE_DEFAULT_STORE_PATH = "/apps/hive-dr/hiveReplication/";
    private static final String DEFAULT_STORE_PATH = BASE_DEFAULT_STORE_PATH + "statusStore/";
    private static final FsPermission DEFAULT_STORE_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    private static final FsPermission DEFAULT_STATUS_FILE_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.READ, FsAction.READ);
    private static final String LATEST_FILE = "latest.json";
    private static final int FILE_ROTATION_LIMIT = 10;
    private static final int FILE_ROTATION_TIME = 86400; // 1 day

    private FileSystem fileSystem;

    public DRStatusStore(FileSystem targetFileSystem) throws IOException {
        this.fileSystem = targetFileSystem;
        Path storePath = new Path(DEFAULT_STORE_PATH);
        try {
            if (!fileSystem.exists(storePath)) {
                FileSystem.mkdirs(fileSystem, storePath, DEFAULT_STORE_PERMISSION);
            }
        } catch (IOException e) {
            LOG.error("mkdir failed for " + DEFAULT_STORE_PATH);
            throw e;
        }
    }

    public void updateReplicationStatus(String jobName, List<ReplicationStatus> statusList)
            throws HiveReplicationException {

        /*
        get all DB updated by the job. get all current table statuses for the DB
        merge the latest repl status with prev table repl statuses.
        if all are success, store the status as success with largest eventId for the DB
        else store status as failure for the DB and lowest eventId
         */

        Map<String, DBReplicationStatus> dbStatusMap = new HashMap<String, DBReplicationStatus>();

        for (ReplicationStatus status : statusList) {
            if (!status.getJobName().equals(jobName)) {
                String error = "JobName for status does not match current job \"" + jobName
                        + "\". Status is " + status.toJsonString();
                LOG.error(error);
                throw new HiveReplicationException(error);
            }
            if (! dbStatusMap.containsKey(status.getDatabase())) {
                dbStatusMap.put(status.getDatabase(),
                        getDbReplicationStatus(status.getSourceUri(), status.getTargetUri(),
                                status.getJobName(), status.getDatabase()));
            }

            if (StringUtils.isEmpty(status.getTable())) {
                // db level replication status.
                dbStatusMap.get(status.getDatabase()).setDbReplicationStatus(status);
            } else {
                // table level replication status
                dbStatusMap.get(status.getDatabase()).getTableStatuses().put(status.getTable(), status);
            }
        }

        for (Map.Entry<String, DBReplicationStatus> dbStatus : dbStatusMap.entrySet()) {
            DBReplicationStatus dbReplicationStatus = dbStatus.getValue();
            dbReplicationStatus.updateDbStatusFromTableStatuses();
            writeStatusFile(dbReplicationStatus);
        }

    }

    public ReplicationStatus getReplicationStatus(String source, String target, String jobName, String database)
            throws HiveReplicationException {
        return getDbReplicationStatus(source, target, jobName, database).getDbReplicationStatus();
    }


    public ReplicationStatus getReplicationStatus(String source, String target,
                                                  String jobName, String database,
                                                  String table) throws HiveReplicationException {
        if (StringUtils.isEmpty(table)) {
            return getReplicationStatus(source, target, jobName, database);
        } else {
            DBReplicationStatus dbReplicationStatus = getDbReplicationStatus(source, target, jobName, database);
            if (dbReplicationStatus.getTableStatuses().containsKey(table)) {
                return dbReplicationStatus.getTableStatuses().get(table);
            }
            return new ReplicationStatus(source, target, jobName, database, table, ReplicationStatus.Status.INIT, -1);
        }
    }


    public Iterator<ReplicationStatus> getTableReplicationStatusesInDb(String source, String target,
                                                                       String jobName, String database)
            throws HiveReplicationException {
        DBReplicationStatus dbReplicationStatus = getDbReplicationStatus(source, target, jobName, database);
        return dbReplicationStatus.getTableStatusIterator();
    }

    private DBReplicationStatus getDbReplicationStatus(String source, String target, String jobName,
                                                       String database) throws HiveReplicationException{
        DBReplicationStatus dbReplicationStatus = null;
        Path statusDirPath = getStatusDirPath(database, jobName);
        // todo check if database name or jobName can contain chars not allowed by hdfs dir/file naming.
        // if yes, use md5 of the same for dir names. prefer to use actual db names for readability.

        try {
            if (fileSystem.exists(statusDirPath)) {
                dbReplicationStatus = readStatusFile(statusDirPath);
            }
            if(null == dbReplicationStatus) {
                dbReplicationStatus = new DBReplicationStatus();
                dbReplicationStatus.setDbReplicationStatus(new ReplicationStatus(source, target, jobName,
                        database, null, ReplicationStatus.Status.INIT, -1));
                if (!FileSystem.mkdirs(fileSystem, statusDirPath, DEFAULT_STORE_PERMISSION)) {
                    String error = "mkdir failed for " + statusDirPath.toString();
                    LOG.error(error);
                    throw new HiveReplicationException(error);
                }
                writeStatusFile(dbReplicationStatus);
            }
            return dbReplicationStatus;
        } catch (IOException e) {
            String error = "Failed to get ReplicationStatus for job " + jobName;
            LOG.error(error);
            throw new HiveReplicationException(error);
        }
    }

    private Path getStatusDirPath(DBReplicationStatus dbReplicationStatus) {
        ReplicationStatus status = dbReplicationStatus.getDbReplicationStatus();
        return getStatusDirPath(status.getDatabase(), status.getJobName());
    }

    private Path getStatusDirPath(String database, String jobName) {
        return new Path(DEFAULT_STORE_PATH + "/" + database + "/" + jobName);
    }

    private void writeStatusFile(DBReplicationStatus dbReplicationStatus) throws HiveReplicationException {
        String statusDir = getStatusDirPath(dbReplicationStatus).toString();
        try {
            Path latestFile = new Path(statusDir + "/" + LATEST_FILE);
            if (fileSystem.exists(latestFile)) {
                Path renamedFile = new Path(statusDir + "/"
                    + String.valueOf(fileSystem.getFileStatus(latestFile).getModificationTime()) + ".json");
                fileSystem.rename(latestFile, renamedFile);
            }

            FSDataOutputStream stream = FileSystem.create(fileSystem, latestFile, DEFAULT_STATUS_FILE_PERMISSION);
            stream.write(dbReplicationStatus.toJsonString().getBytes());
            stream.close();

        } catch (IOException e) {
            String error = "Failed to write latest Replication status into dir " + statusDir;
            LOG.error(error);
            throw new HiveReplicationException(error);
        }

        rotateStatusFiles(new Path(statusDir));
    }

    private void rotateStatusFiles(Path statusDir) throws HiveReplicationException {

        List<String> fileList = new ArrayList<String>();
        long now = System.currentTimeMillis();
        try {
            RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(statusDir, false);
            while (fileIterator.hasNext()) {
                fileList.add(String.valueOf(fileIterator.next().getModificationTime()));
            }
            if (fileList.size() > FILE_ROTATION_LIMIT) {
                // delete some files, as long as they are older than the time.
                Collections.sort(fileList);
                for (String file : fileList.subList(0, fileList.size() - FILE_ROTATION_LIMIT)) {
                    long modTime = Long.getLong(file);
                    if ((now - modTime) > FILE_ROTATION_TIME) {
                        Path deleteFilePath = new Path(statusDir.toString() + "/" + file + ".json");
                        if (fileSystem.exists(deleteFilePath)) {
                            fileSystem.delete(deleteFilePath, false);
                        }
                    }
                }
            }
        } catch (IOException e) {
            String error = "Failed to rotate status files in dir " + statusDir.toString();
            LOG.error(error);
            throw new HiveReplicationException(error);
        }
    }

    private DBReplicationStatus readStatusFile(Path statusDirPath) throws HiveReplicationException {
        try {
            Path statusFile = new Path(statusDirPath.toString() + "/" + LATEST_FILE);
            if ((!fileSystem.exists(statusDirPath)) || (!fileSystem.exists(statusFile))) {
                return null;
            } else {
                return new DBReplicationStatus(IOUtils.toString(fileSystem.open(statusFile)));
            }
        } catch (IOException e) {
            String error = "Failed to read latest Replication status from dir " + statusDirPath.toString();
            LOG.error(error);
            throw new HiveReplicationException(error);
        }
    }

    private boolean checkForReplicationConflict(String source, String target, String database, String table){
        // todo
        return false;
    }


}
