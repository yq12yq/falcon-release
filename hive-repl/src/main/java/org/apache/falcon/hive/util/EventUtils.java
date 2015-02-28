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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to handle Hive events for data-mirroring.
 */
public class EventUtils {
    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final int TIMEOUT_IN_SECS = 300;
    private static final String JDBC_PREFIX = "jdbc:";
    private Configuration conf = null;
    private String sourceHiveServer2Uri = null;
    private String sourceDatabase = null;
    private String sourceStagingPath = null;
    private String sourceNN = null;
    private String targetHiveServer2Uri = null;
    private String targetStagingPath = null;
    private String targetNN = null;
    private String sourceStagingUri = null;
    private String targetStagingUri = null;
    private List<String> sourceCleanUpList = null;
    private List<String> targetCleanUpList = null;
    private static final Logger LOG = LoggerFactory.getLogger(EventUtils.class);

    private FileSystem targetFileSystem = null;
    private Connection sourceConnection = null;
    private Connection targetConnection = null;
    private Statement sourceStatement = null;
    private Statement targetStatement = null;

    private List<ReplicationStatus> listReplicationStatus;

    public EventUtils(Configuration conf) {
        this.conf = conf;
        sourceHiveServer2Uri = conf.get("sourceHiveServer2Uri");
        sourceDatabase = conf.get("sourceDatabase");
        sourceStagingPath = conf.get("sourceStagingPath");
        sourceNN = conf.get("sourceNN");

        targetHiveServer2Uri = conf.get("targetHiveServer2Uri");
        targetStagingPath = conf.get("targetStagingPath");
        targetNN = conf.get("targetNN");
        sourceCleanUpList = new ArrayList<String>();
        targetCleanUpList = new ArrayList<String>();
    }

    public void setupConnection() throws Exception {
        Class.forName(DRIVER_NAME);
        DriverManager.setLoginTimeout(TIMEOUT_IN_SECS);

        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        String user = "";
        if (currentUser != null) {
            user = currentUser.getShortUserName();
        }
        sourceConnection = DriverManager.getConnection(JDBC_PREFIX + sourceHiveServer2Uri + "/" + sourceDatabase,
                user, "");
        targetConnection = DriverManager.getConnection(JDBC_PREFIX + targetHiveServer2Uri + "/" + sourceDatabase,
                user, "");
        sourceStatement = sourceConnection.createStatement();
        targetStatement = targetConnection.createStatement();
    }

    public void initializeFS() throws IOException {
        LOG.info("Initializing staging directory");
        sourceStagingUri = sourceNN + sourceStagingPath;
        targetStagingUri = targetNN + targetStagingPath;
        targetFileSystem = FileSystem.get(FileUtils.getConfiguration(targetNN));
    }

    public void processEvents(String event) throws Exception {
        LOG.info("EventUtils processEvents event to process: {}", event);
        listReplicationStatus = new ArrayList<ReplicationStatus>();
        String[] eventSplit = event.split(DelimiterUtils.FIELD_DELIM);
        String dbName = eventSplit[0];
        String tableName = eventSplit[1];
        String exportEventStr = eventSplit[2];
        String importEventStr = eventSplit[3];
        if (StringUtils.isNotEmpty(exportEventStr)) {
            LOG.info("Process the export statements");
            processCommands(exportEventStr, dbName, tableName, sourceStatement, sourceCleanUpList, false);
            //TODO Check srcStagingDirectory is not empty
            invokeCopy();
        }

        if (StringUtils.isNotEmpty(importEventStr)) {
            LOG.info("Process the import statements");
            processCommands(importEventStr, dbName, tableName, targetStatement, targetCleanUpList, true);
        }
    }

    public List<ReplicationStatus> getListReplicationStatus() {
        return listReplicationStatus;
    }

    private void processCommands(String eventStr, String dbName, String tableName, Statement sqlStmt,
                                 List<String> cleanUpList, boolean updateStatus)
            throws SQLException, HiveReplicationException {
        String[] commandList = eventStr.split(DelimiterUtils.STMT_DELIM);
        for (String command : commandList) {
            LOG.debug(" Hive DR Deserialize : {} :", command);
            try {
                Command cmd = ReplicationUtils.deserializeCommand(command);
                cleanUpList.addAll(cmd.cleanupLocationsAfterEvent());
                executeCommand(cmd, dbName, tableName, sqlStmt, updateStatus);
            } catch (IOException ioe) {
                throw new HiveReplicationException("Could not deserialize replication command for "
                        + " DB Name:" + dbName + ", Table Name:" + tableName, ioe);
            }
        }
    }

    private void executeCommand(Command cmd, String dbName, String tableName, Statement sqlStmt, boolean updateStatus)
            throws HiveReplicationException, SQLException {
        for (final String stmt : cmd.get()) {
            try {
                sqlStmt.execute(stmt);
            } catch (SQLException sqeOuter) {
                LOG.error("SQL Exception: {}", sqeOuter);
                if (cmd.isUndoable()) {
                    try {
                        undoCommands(cmd.getUndo(), sqlStmt);
                    } catch (SQLException sqeInner) {
                        if (updateStatus) {
                            addReplicationStatus(ReplicationStatus.Status.FAILURE, dbName,
                                    tableName, cmd.getEventId());
                        }
                        throw sqeInner;
                    }
                }
                if (updateStatus) {
                    addReplicationStatus(ReplicationStatus.Status.FAILURE, dbName, tableName, cmd.getEventId());
                }
                throw sqeOuter;
            }
        }
        if (updateStatus) {
            addReplicationStatus(ReplicationStatus.Status.SUCCESS, dbName, tableName, cmd.getEventId());
        }
    }

    private void undoCommands(List<String> undoCommands, Statement sqlStmt) throws SQLException {
        LOG.info("Undo command: {}", StringUtils.join(undoCommands.toArray()));
        if (undoCommands.size() != 0) {
            for (final String undoStmt : undoCommands) {
                sqlStmt.execute(undoStmt);
            }
        }
    }

    private void addReplicationStatus(ReplicationStatus.Status status, String dbName, String tableName, long eventId)
            throws HiveReplicationException {
        try {
            String drJobName = conf.get("drJobName");
            ReplicationStatus rs = new ReplicationStatus(conf.get("sourceCluster"), conf.get("targetCluster"),
                    drJobName, dbName, tableName, status, eventId);
            listReplicationStatus.add(rs);
        } catch (HiveReplicationException hre) {
            throw new HiveReplicationException("Could not update replication status store for "
                    + " EventId:" + eventId
                    + " DB Name:" + dbName
                    + " Table Name:" + tableName
                    + hre.toString());
        }
    }

    public void invokeCopy() throws Exception {
        DistCpOptions options = getDistCpOptions();
        DistCp distCp = new DistCp(conf, options);
        LOG.info("Started DistCp with source Path: {} \ttarget path: ", options.getSourcePaths().toString(),
                options.getTargetPath());
        Job distcpJob = distCp.execute();
        LOG.info("Distp Hadoop job: {}", distcpJob.getJobID().toString());
        LOG.info("Completed DistCp");
    }

    public DistCpOptions getDistCpOptions() {
        String[] paths = (sourceStagingUri).trim().split(",");
        List<Path> srcPaths = getPaths(paths);

        DistCpOptions distcpOptions = new DistCpOptions(srcPaths, new Path(targetStagingUri));
        distcpOptions.setSyncFolder(true);
        distcpOptions.setBlocking(true);
        distcpOptions.setMaxMaps(Integer.valueOf(conf.get("maxMaps")));
        distcpOptions.setMapBandwidth(Integer.valueOf(conf.get("mapBandwidth")));
        return distcpOptions;
    }

    private List<Path> getPaths(String[] paths) {
        List<Path> listPaths = new ArrayList<Path>();
        for (String path : paths) {
            listPaths.add(new Path(path));
        }
        return listPaths;
    }

    public void cleanEventsDirectory() throws IOException {
        LOG.info("Cleaning staging directory");
        try {
            for (String cleanUpPath : sourceCleanUpList) {
                String cleanUpStmt = "dfs -rmr " + cleanUpPath;
                sourceStatement.execute(cleanUpStmt);
            }
        } catch (SQLException e) {
            LOG.error("Cleaning up of source staging directory failed", e);
        }

        try {
            for (String cleanUpPath : targetCleanUpList) {
                targetFileSystem.delete(new Path(cleanUpPath), true);
            }
        } catch (IOException e) {
            LOG.error("Cleaning up of target staging directory failed", e);
        }
    }

    public void closeConnection() throws SQLException {
        if (sourceStatement != null) {
            sourceStatement.close();
        }

        if (targetStatement != null) {
            targetStatement.close();
        }

        if (sourceConnection != null) {
            sourceConnection.close();
        }
        if (targetConnection != null) {
            targetConnection.close();
        }
    }
}
