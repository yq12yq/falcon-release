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

import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class EventUtils {
    public static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String SASL_AUTH = ";auth=noSasl";
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
    private String targetDatabase = null;;
    private String targetTable = null;
    private String targetStagingPath = null;
    private String targetNN = null;
    private String targetRM = null;
    private DRStatusStore hiveStore = null;
    private String sourceStagingUri = null;
    private String targetStagingUri = null;
    private String jobName = null;
    private int maxEvents;

    FileSystem fs = null;
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
        targetDatabase = conf.get("targetDatabase");
        targetTable = conf.get("targetTable");
        targetStagingPath = conf.get("targetStagingPath");
        targetNN = conf.get("targetNN");
        targetRM = conf.get("targetRM");
        jobName = conf.get("jobName");
    }

    public void setupConnection() {
        try {
            src_con = DriverManager.getConnection(JDBC_PREFIX+sourceHiveServer2Uri+"/"+sourceDatabase+SASL_AUTH,
                    sourceServicePrincipal, "");
            tgt_con = DriverManager.getConnection(JDBC_PREFIX+targetHiveServer2Uri+"/"+targetDatabase+SASL_AUTH,
                    targetServicePrincipal, "");
            src_stmt = src_con.createStatement();
            tgt_stmt = tgt_con.createStatement();
        } catch (SQLException e) {
            System.out.println("Exception while establishing connection");
            e.printStackTrace();
        }
    }

    public void initializeFS() {
        sourceStagingUri = sourceNN + sourceStagingPath;
        targetStagingUri = targetNN + targetStagingPath;

        System.out.println("Initializing staging directory");
        System.out.println("sourceStatingDirPath:"+sourceStagingUri);
        System.out.println("targetStatingDirPath:"+targetStagingUri);

        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initializeHiveDRStore() throws IOException {
        hiveStore = new HiveDRStatusStore(fs);
    }

    public void processEvents(String event) throws Exception {
        ReplicationStatus.Status status = null;
        listReplicationStatus = new ArrayList<ReplicationStatus>() ;
        String dbName = event.split(DelimiterUtils.getEscapedFieldDelim())[0];
        String tableName = event.split(DelimiterUtils.getEscapedFieldDelim())[1];
        String exportEventStr = event.split(DelimiterUtils.getEscapedFieldDelim())[2];
        String importEventStr = event.split(DelimiterUtils.getEscapedFieldDelim())[3];
        if (exportEventStr != "" || exportEventStr != null) {
            processCommands(exportEventStr, dbName, tableName, src_stmt);
            //Todo: Check srcStagingDirectory is not empty
            //invokeCopy();
        }

        if(importEventStr != "" || importEventStr != null ) {
            processCommands(importEventStr, dbName, tableName, tgt_stmt);
        }
    }

    public List<ReplicationStatus> getListReplicationStatus() {
        return listReplicationStatus;
    }

    private void processCommands(String eventStr, String dbName, String tableName, Statement sql_stmt)
            throws Exception {
        long eventId = 0;
        ReplicationStatus.Status status = null;
        String commandList[] = eventStr.split(DelimiterUtils.getEscapedStmtDelim());
        for (String command : commandList) {
            Command cmd = ReplicationUtils.deserializeCommand(command);
            eventId = cmd.getEventId();
            try {
                for (String stmt : cmd.get()) {
                    sql_stmt.execute(stmt);
                }
                status = ReplicationStatus.Status.SUCCESS;
                addReplicationStatus(status, dbName, tableName, eventId);
            } catch (Exception e) {
                if(cmd.isUndoable()) {
                    undoCommands(cmd);
                }
                status = ReplicationStatus.Status.FAILURE;
                addReplicationStatus(status, dbName, tableName, eventId);
            }
        }
    }

    private void undoCommands(Command event) {
        System.out.println("Undo command:" +event.toString());
        return ;
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
        DistCp distCp = new DistCp(conf, options);
        System.out.println("Started DistCp");
        distCp.execute();
        System.out.println("Completed DistCp");

        return ;
    }

    public DistCpOptions getDistCpOptions() {
        String[] paths = conf.get("sourceStagingPath").trim().split(",");
        List<Path> srcPaths = getPaths(paths);
        String trgPath = conf.get("targetStagingPath");

        DistCpOptions distcpOptions = new DistCpOptions(srcPaths, new Path(trgPath));
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

    public void closeConnection() throws SQLException,IOException {
        src_con.close();
        tgt_con.close();
    }
}
