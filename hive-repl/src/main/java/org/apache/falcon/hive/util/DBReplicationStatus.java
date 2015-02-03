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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DBReplicationStatus {

    private static final Logger LOG = LoggerFactory.getLogger(DBReplicationStatus.class);
    private static final String DB_STATUS = "db_status";
    private static final String DB_NAME = "db_name";
    private static final String TABLE_STATUS = "table_status";

    private Map<String, ReplicationStatus> tableStatuses = new HashMap<String, ReplicationStatus>();
    private ReplicationStatus dbReplicationStatus;

    public DBReplicationStatus() {}

    // de-serialize
    public DBReplicationStatus(String jsonString) throws HiveReplicationException {
        try {
            JSONObject object = new JSONObject(jsonString);
            setDbReplicationStatus(new ReplicationStatus(object.get(DB_STATUS).toString()));
            JSONObject tableJson = object.getJSONObject(TABLE_STATUS);
            Iterator keys = tableJson.keys();
            while(keys.hasNext()) {
                String key = keys.next().toString();
                tableStatuses.put(key, new ReplicationStatus(tableJson.get(key).toString()));
            }
        } catch (JSONException e) {
            throw new HiveReplicationException("Unable to create DBReplicationStatus from JsonString", e);
        }
    }


    public Map<String, ReplicationStatus> getTableStatuses() {
        return tableStatuses;
    }

    public Iterator<ReplicationStatus> getTableStatusIterator() {
        List<ReplicationStatus> resultSet = new ArrayList<ReplicationStatus>();
        for (Map.Entry<String, ReplicationStatus> entry : tableStatuses.entrySet()) {
            resultSet.add(entry.getValue());
        }
        return resultSet.iterator();
    }

    public void setTableStatuses(Map<String, ReplicationStatus> tableStatuses) {
        this.tableStatuses = tableStatuses;
    }

    public ReplicationStatus getDbReplicationStatus() {
        return dbReplicationStatus;
    }

    public void setDbReplicationStatus(ReplicationStatus dbReplicationStatus) {
        this.dbReplicationStatus = dbReplicationStatus;
    }

    // Serialize
    public String toJsonString() throws HiveReplicationException {
        JSONObject retObject = new JSONObject();
        JSONObject tableStatus = new JSONObject();
        try {
            for (Map.Entry<String, ReplicationStatus> status : tableStatuses.entrySet()) {
                tableStatus.put(status.getKey(), status.getValue().toJsonObject());
            }
            retObject.put(DB_NAME, dbReplicationStatus.getDatabase());
            retObject.put(DB_STATUS, dbReplicationStatus.toJsonObject());
            retObject.put(TABLE_STATUS, tableStatus);
            return retObject.toString(ReplicationStatus.INDENT_FACTOR);
        } catch (JSONException e) {
            throw new HiveReplicationException("Unable to serialize Database Replication Status", e);
        }
    }

   /**
     * Update DB status.
            case 1) All tables replicated successfully.
                Take the largest successful eventId and set dbReplStatus as success
            case 2) One or many tables failed to replicate
                Take the smallest eventId amongst the failed tables and set dbReplStatus as failed.
     * @return
     * destination commands for each table
     */
    public void updateDbStatusFromTableStatuses() throws HiveReplicationException {

        dbReplicationStatus.setStatus(ReplicationStatus.Status.SUCCESS);
        long successEventId = dbReplicationStatus.getEventId();
        long failedEventId = -1;

        for (Map.Entry<String, ReplicationStatus> entry : tableStatuses.entrySet()) {
            long eventId = entry.getValue().getEventId();
            if (entry.getValue().getStatus().equals(ReplicationStatus.Status.SUCCESS)) {
                if (eventId > successEventId) {
                    successEventId = eventId;
                }
            } else if (entry.getValue().getStatus().equals(ReplicationStatus.Status.FAILURE)) {
                dbReplicationStatus.setStatus(ReplicationStatus.Status.FAILURE);
                if (eventId < failedEventId) {
                    failedEventId = eventId;
                }
            }
        }

        String info = "Updating DB Status based on table replication status. Status : "
                + dbReplicationStatus.getStatus().toString() + ", eventId : ";
        if (dbReplicationStatus.getStatus().equals(ReplicationStatus.Status.SUCCESS)) {
            dbReplicationStatus.setEventId(successEventId);
            LOG.info(info + String.valueOf(successEventId));
        } else if (dbReplicationStatus.getStatus().equals(ReplicationStatus.Status.FAILURE)) {
            dbReplicationStatus.setEventId(failedEventId);
            LOG.info(info + String.valueOf(failedEventId));
        }

    }
}
