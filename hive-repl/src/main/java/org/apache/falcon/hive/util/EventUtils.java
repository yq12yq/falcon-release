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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to process hive events.
 */
public class EventUtils {
    public static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
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
    private FileSystem sourceFileSystem = null;
    private Connection sourceConnection = null;
    private Connection targetConnection = null;

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
        sourceConnection = DriverManager.getConnection(JDBC_PREFIX + sourceHiveServer2Uri
                + "/" + sourceDatabase);
        targetConnection = DriverManager.getConnection(JDBC_PREFIX + targetHiveServer2Uri
                + "/" + sourceDatabase);
    }

    public void initializeFS() throws IOException {
        LOG.info("Initializing staging directory");
        sourceStagingUri = sourceNN + sourceStagingPath;
        targetStagingUri = targetNN + targetStagingPath;

        sourceFileSystem = FileSystem.get(FileUtils.getConfiguration(sourceNN));
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
            processCommands(exportEventStr, dbName, tableName, sourceConnection, sourceCleanUpList);
            //TODO Check srcStagingDirectory is not empty
            invokeCopy();
        }

        if (StringUtils.isNotEmpty(importEventStr)) {
            LOG.info("Process the import statements");
            processCommands(importEventStr, dbName, tableName, targetConnection, targetCleanUpList);
        }
    }

    public List<ReplicationStatus> getListReplicationStatus() {
        return listReplicationStatus;
    }

    private void processCommands(String eventStr, String dbName, String tableName, Connection connection,
                                 List<String> cleanUpList) throws SQLException, HiveReplicationException {
        long eventId;
        ReplicationStatus.Status status;
        String[] commandList = eventStr.split(DelimiterUtils.STMT_DELIM);
        for (String command : commandList) {
            LOG.debug(" Hive DR Deserialize : {} :", command);
            Command cmd;
            try {
                cmd = ReplicationUtils.deserializeCommand(command);
            } catch (IOException ioe) {
                throw new HiveReplicationException("Could not deserialize replication command for "
                        + " DB Name:" + dbName + ", Table Name:" + tableName, ioe);
            }
            eventId = cmd.getEventId();
            cleanUpList.addAll(cmd.cleanupLocationsAfterEvent());
            for (String stmt : cmd.get()) {
                PreparedStatement sqlStmt = connection.prepareStatement(stmt);
                try {
                    sqlStmt.execute();
                } catch (SQLException sqeOuter) {
                    LOG.error("SQL Exception: {}", sqeOuter);
                    if (cmd.isUndoable()) {
                        try {
                            undoCommands(cmd.getUndo(), connection);
                        } catch (SQLException sqeInner) {
                            addReplicationStatus(ReplicationStatus.Status.FAILURE, dbName, tableName, eventId);
                            sqlStmt.close();
                            throw sqeInner;
                        }
                    }
                    addReplicationStatus(ReplicationStatus.Status.FAILURE, dbName, tableName, eventId);
                    throw sqeOuter;
                } finally {
                    sqlStmt.close();
                }
                status = ReplicationStatus.Status.SUCCESS;
                addReplicationStatus(status, dbName, tableName, eventId);
            }
        }
    }

    private void undoCommands(List<String> undo, Connection connection) throws SQLException {
        LOG.info("Undo command: {}", StringUtils.join(undo.toArray()));
        if (undo.size() != 0) {
            for (String undoStmt : undo) {
                PreparedStatement sqlStmt = connection.prepareStatement(undoStmt);
                try {
                    sqlStmt.execute();
                }  finally {
                    sqlStmt.close();
                }
            }
        }
    }

    private void addReplicationStatus(ReplicationStatus.Status status, String dbName, String tableName, long eventId)
        throws HiveReplicationException {
        String drJobName = conf.get("drJobName");
        ReplicationStatus rs = new ReplicationStatus(conf.get("sourceCluster"), conf.get("targetCluster"), drJobName,
                dbName, tableName, status, eventId);
        listReplicationStatus.add(rs);
    }

    public void invokeCopy() throws Exception {
        DistCpOptions options = getDistCpOptions();

        Configuration configuration = new Configuration();
        // inject wf configs
        Path confPath = new Path("file:///"
                + System.getProperty("oozie.action.conf.xml"));

        LOG.info("{} found conf ? {}", confPath, confPath.getFileSystem(configuration).exists(confPath));
        configuration.addResource(confPath);

        DistCp distCp = new DistCp(configuration, options);
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
                PreparedStatement srcStmt = sourceConnection.prepareStatement(cleanUpStmt);
                srcStmt.execute();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }

        for (String cleanUpPath : targetCleanUpList) {
            targetFileSystem.delete(new Path(cleanUpPath), true);
        }
    }

    public void closeConnection() throws SQLException {
        if (sourceConnection != null) {
            sourceConnection.close();
        }
        if (targetConnection != null) {
            targetConnection.close();
        }
    }
}
