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

package org.apache.falcon.ADFService;

import org.apache.falcon.FalconException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Azure ADF base job
 */
public abstract class ADFJob {

    // name prefix for all adf related entity, e.g. an adf hive process and the feeds associated with it
    public static final String ADF_ENTITY_NAME_PREFIX = "ADF_";
    // name prefix for all adf related job entity, i.e. adf hive/pig process and replication feed
    public static final String ADF_JOB_ENTITY_NAME_PREFIX = ADF_ENTITY_NAME_PREFIX + "JOB_";
    public static final int ADF_ENTITY_NAME_PREFIX_LENGTH = ADF_ENTITY_NAME_PREFIX.length();

    public static boolean isADFEntity(String entityName) {
        return entityName.startsWith(ADF_ENTITY_NAME_PREFIX);
    }

    public static boolean isADFJobEntity(String entityName) {
        return entityName.startsWith(ADF_JOB_ENTITY_NAME_PREFIX);
    }

    public static String getSessionID(String entityName) throws FalconException {
        if (!isADFJobEntity(entityName)) {
            throw new FalconException("The entity, " + entityName + ", is not an ADF Job Entity.");
        }
        return entityName.substring(ADF_ENTITY_NAME_PREFIX_LENGTH);
    }

    public static enum JobType {
        HIVE, PIG, REPLICATE
    }

    public static JobType getJobType(String msg) throws FalconException {
        try {
            JSONObject obj = new JSONObject(msg);
            JSONObject activityProperties = obj.getJSONObject("activity").getJSONObject("transformation");
            String type = activityProperties.getString("type");
            switch (type) {
                case "HadoopReplicateData":
                    return JobType.REPLICATE;
                case "HadoopHive":
                    return JobType.HIVE;
                case "HadoopPig":
                    return JobType.PIG;
                default:
                    throw new FalconException("Unrecognized ADF job type: " + type);
            }
        } catch (JSONException e) {
            throw new FalconException("Error when parsing ADF JSON message: " + msg, e);
        }
    }

    public abstract void submitJob();

    private JSONObject message;
    private String id;
    private JobType type;

    public ADFJob(String msg) throws FalconException {
        try {
            message = new JSONObject(msg);
            id = message.getString("jobId");
        } catch (JSONException e) {
            throw new FalconException("Error when parsing ADF JSON message: " + msg, e);
        }
    }

    public String jobEntityName() {
        return ADF_JOB_ENTITY_NAME_PREFIX + id;
    }

    public String sessionID() {
        return id;
    }

    public JobType jobType() {
        return type;
    }
}
