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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class EventUtils {
    public static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final int TIMEOUT_IN_SECS = 300;
    private static final String JDBC_PREFIX = "jdbc:";
    private Configuration conf = null;
    private String sourceHiveServer2Uri = null;
    private String sourceServicePrincipal = null;
    private String sourceDatabase = null;
    private String sourceTable = null;
    private String sourceStagingPath = null;
    private String sourceNN = null;
    private String sourceRM = null;
    private String targetHiveServer2Uri = null;
    private String targetServicePrincipal = null;
    private String targetStagingPath = null;
    private String targetNN = null;
    private String targetRM = null;
    private DRStatusStore hiveStore = null;
    private String sourceStagingUri = null;
    private String targetStagingUri = null;
    private String jobName = null;
    private List<String> sourceCleanUpList = null;
    private List<String> targetCleanUpList = null;
    private int maxEvents;
    private static final Logger LOG = LoggerFactory.getLogger(EventUtils.class);

    FileSystem trgFs = null;
    FileSystem srcFs = null;
    static Connection src_con = null;
    static Connection tgt_con = null;
    static Statement src_stmt = null;
    static Statement tgt_stmt = null;
    private String fsName = null;

    private List<ReplicationStatus> listReplicationStatus;

    static {
        try {
            Class.forName(DRIVER_NAME);
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public EventUtils(Configuration conf) {
        this.conf = conf;
        this.fsName = conf.get("fs.default.name");
        sourceHiveServer2Uri = conf.get("sourceHiveServer2Uri");
        sourceServicePrincipal = conf.get("sourceServicePrincipal");
        sourceDatabase = conf.get("sourceDatabase");
        sourceTable = conf.get("sourceTable");
        sourceStagingPath = conf.get("sourceStagingPath");
        sourceNN = conf.get("sourceNN");
        sourceRM = conf.get("sourceRM");

        targetHiveServer2Uri = conf.get("targetHiveServer2Uri");
        targetServicePrincipal = conf.get("targetServicePrincipal");
        targetStagingPath = conf.get("targetStagingPath");
        targetNN = conf.get("targetNN");
        targetRM = conf.get("targetRM");
        jobName = conf.get("jobName");
        sourceCleanUpList = new ArrayList<String>();
        targetCleanUpList = new ArrayList<String>();
    }

    public void setupConnection() {
        try {
            DriverManager.setLoginTimeout(TIMEOUT_IN_SECS);
            src_con = DriverManager.getConnection(JDBC_PREFIX+sourceHiveServer2Uri+"/"+sourceDatabase, "", "");
            tgt_con = DriverManager.getConnection(JDBC_PREFIX+targetHiveServer2Uri+"/"+sourceDatabase, "", "");
            src_stmt = src_con.createStatement();
            tgt_stmt = tgt_con.createStatement();
        } catch (SQLException e) {
            LOG.info("Exception while establishing connection");
            e.printStackTrace();
        }
    }

    public void initializeFS() {
        LOG.info("Initializing staging directory");
        sourceStagingUri = sourceNN + sourceStagingPath;
        targetStagingUri = targetNN + targetStagingPath;

        try {
            srcFs = FileSystem.get(FileUtils.getConfiguration(sourceNN));
            trgFs = FileSystem.get(FileUtils.getConfiguration(targetNN));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initializeHiveDRStore() throws IOException {
        hiveStore = new HiveDRStatusStore(trgFs);
    }

    public void processEvents(String event) throws Exception{
        System.out.println("EventUtils processEvents event to process:"+event);
        listReplicationStatus = new ArrayList<ReplicationStatus>() ;
        String eventSplit[] = event.split(DelimiterUtils.getRecordFieldDelim());
        String dbName = eventSplit[0];
        String tableName = eventSplit[1];
        String exportEventStr = eventSplit[2];
        String importEventStr = eventSplit[3];
        if (StringUtils.isNotEmpty(exportEventStr)) {
            processCommands(exportEventStr, dbName, tableName, src_stmt, sourceCleanUpList);
            //Todo: Check srcStagingDirectory is not empty
            invokeCopy();
        }

        if(StringUtils.isNotEmpty(importEventStr)) {
            processCommands(importEventStr, dbName, tableName, tgt_stmt, targetCleanUpList);
        }
    }

    public List<ReplicationStatus> getListReplicationStatus() {
        return listReplicationStatus;
    }

    private void processCommands(String eventStr, String dbName, String tableName, Statement sql_stmt,
                                 List<String> cleanUpList) throws SQLException, HiveReplicationException {
        long eventId = 0;
        ReplicationStatus.Status status = null;
        String commandList[] = eventStr.split(DelimiterUtils.getEventStmtDelim());
        for (String command : commandList) {
            ReplicationCommand cmd = ReplicationCommand.parseCommandString(command);
            eventId = cmd.getEventId();
            cleanUpList.addAll(cmd.cleanupLocationsAfterEvent());
            try {
                for (String stmt : cmd.get()) {
                    sql_stmt.execute(stmt);
                }
                status = ReplicationStatus.Status.SUCCESS;
                addReplicationStatus(status, dbName, tableName, eventId);
            } catch (SQLException e) {
                LOG.info("SQL Exception :"+e.toString());
                if(cmd.isUndoable()) {
                    undoCommands(cmd.getUndo(),sql_stmt);
                }
                status = ReplicationStatus.Status.FAILURE;
                addReplicationStatus(status, dbName, tableName, eventId);
            } catch (HiveReplicationException hre) {
                throw new HiveReplicationException("Could not update replication status store for "
                        + "EventId:" + eventId
                        + "DB Name:" + dbName
                        + "Table Name:" + tableName
                        + hre.toString());
            }
        }
    }

    private void undoCommands(List<String> undo, Statement sql_stmt) throws HiveReplicationException {
        LOG.info("Undo command:" + undo.toString());
        try {
            if (undo.size() != 0) {
                for (String undoStmt : undo) {
                    sql_stmt.execute(undoStmt);
                }
            }
        } catch(SQLException se) {
            throw new HiveReplicationException("Could not undo Hive statement:"+se.toString());
        }
    }

    private void addReplicationStatus(ReplicationStatus.Status status, String dbName, String tableName, long eventId)
            throws HiveReplicationException{
        String drJobName = conf.get("drJobName");
        ReplicationStatus rs = new ReplicationStatus(conf.get("sourceCluster"), conf.get("targetCluster"), drJobName,
                dbName, tableName, status, eventId);
        listReplicationStatus.add(rs);
    }

    public void invokeCopy() throws Exception {
        DistCpOptions options = getDistCpOptions();

        Configuration conf = new Configuration();
        // inject wf configs
        Path confPath = new Path("file:///"
                + System.getProperty("oozie.action.conf.xml"));

        LOG.info("{} found conf ? {}", confPath, confPath.getFileSystem(conf).exists(confPath));
        conf.addResource(confPath);

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

    public void cleanStagingDir() throws IOException {
        LOG.info("Cleaning staging directory");
        for(String cleanUpPath : sourceCleanUpList) {
            srcFs.delete(new Path(cleanUpPath), true);
        }

        for(String cleanUpPath : targetCleanUpList) {
            trgFs.delete(new Path(cleanUpPath), true);
        }
    }

    public void closeConnection() throws SQLException {
        if (src_con != null) {
            src_con.close();
        }
        if (tgt_con != null) {
            tgt_con.close();
        }
    }
}
