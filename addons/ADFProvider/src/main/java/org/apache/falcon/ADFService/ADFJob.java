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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.falcon.ADFService.util.ADFJsonConstants;
import org.apache.falcon.ADFService.util.FSUtils;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.FalconException;
import org.apache.falcon.resource.AbstractSchedulableEntityManager;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure ADF base job.
 */
public abstract class ADFJob {
    private static final Logger LOG = LoggerFactory.getLogger(ADFJob.class);

    // name prefix for all adf related entity, e.g. an adf hive process and the feeds associated with it
    public static final String ADF_ENTITY_NAME_PREFIX = "ADF-";
    // name prefix for all adf related job entity, i.e. adf hive/pig process and replication feed
    public static final String ADF_JOB_ENTITY_NAME_PREFIX = ADF_ENTITY_NAME_PREFIX + "JOB-";
    public static final int ADF_ENTITY_NAME_PREFIX_LENGTH = ADF_ENTITY_NAME_PREFIX.length();

    // TODO(yzheng): read falcon url from client.properties
    protected static final String DEFAULT_FALCON_URL = "http://127.0.0.1:15000/";
    public static final String HDFS_URL_PORT = "hdfs://sandbox.hortonworks.com:8020";
    public static final String TEMPLATE_PATH_PREFIX = "/apps/falcon/";
    public static final String PROCESS_SCRIPTS_PATH = TEMPLATE_PATH_PREFIX
            + Path.SEPARATOR + "generatedscripts";

    // Constants for process properties
    private static final String PROPERTY_START = "<property ";
    private static final String PROPERTY_NAME = "name=";
    private static final String PROPERTY_VALUE = "value=";
    private static final String PROPERTY_END = "/>";
    private static final String PROPERTY_DOUBLE_QUOTES = "\"";


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

    /**
     * Enum for job type.
     */
    public static enum JobType {
        HIVE, PIG, REPLICATION
    }

    private static enum RequestType {
        HADOOPREPLICATEDATA, HADOOPHIVE, HADOOPPIG
    }

    public static JobType getJobType(String msg) throws FalconException {
        try {
            JSONObject obj = new JSONObject(msg);
            JSONObject activity = obj.getJSONObject(ADFJsonConstants.ADF_REQUEST_ACTIVITY);
            if (activity == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_ACTIVITY + " not found in ADF"
                        + " request.");
            }

            JSONObject activityProperties = activity.getJSONObject(ADFJsonConstants.ADF_REQUEST_TRANSFORMATION);
            if (activityProperties == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_TRANSFORMATION + " not found "
                        + "in ADF request.");
            }

            String type = activityProperties.getString(ADFJsonConstants.ADF_REQUEST_TYPE);
            if (StringUtils.isBlank(type)) {
                throw new FalconException(ADFJsonConstants.ADF_REQUEST_TYPE + " not found in ADF request msg");
            }

            switch (RequestType.valueOf(type.toUpperCase())) {
            case HADOOPREPLICATEDATA:
                return JobType.REPLICATION;
            case HADOOPHIVE:
                return JobType.HIVE;
            case HADOOPPIG:
                return JobType.PIG;
            default:
                throw new FalconException("Unrecognized ADF job type: " + type);
            }
        } catch (JSONException e) {
            throw new FalconException("Error when parsing ADF JSON message: " + msg, e);
        }
    }

    public abstract void startJob() throws FalconException;

    protected JSONObject message;
    protected JSONObject activity;
    protected JSONObject activityExtendedProperties;
    protected String id;
    protected JobType type;
    protected String startTime, endTime;
    protected String frequency;
    protected String proxyUser;
    protected ADFJobManager jobManager = new ADFJobManager();

    private Map<String, JSONObject> linkedServicesMap = new HashMap<String, JSONObject>();
    protected Map<String, JSONObject> tablesMap = new HashMap<String, JSONObject>();

    public ADFJob(String msg, String id) throws FalconException {
        this.id = id;
        FSUtils.createScriptDir(new Path(TEMPLATE_PATH_PREFIX));
        try {
            this.id = id;
            message = new JSONObject(msg);

            frequency = "days(1)";
            startTime = message.getString(ADFJsonConstants.ADF_REQUEST_START_TIME);
            startTime = startTime.substring(0, startTime.length()-4) + "Z";
            //TODO(yzheng): set to the next day of start time
            endTime = message.getString(ADFJsonConstants.ADF_REQUEST_END_TIME);
            endTime = endTime.substring(0, endTime.length()-4) + "Z";

            JSONArray linkedServices = message.getJSONArray(ADFJsonConstants.ADF_REQUEST_LINKED_SERVICES);
            for (int i = 0; i < linkedServices.length(); i++) {
                JSONObject linkedService = linkedServices.getJSONObject(i);
                linkedServicesMap.put(linkedService.getString(ADFJsonConstants.ADF_REQUEST_NAME), linkedService);
            }

            JSONArray tables = message.getJSONArray(ADFJsonConstants.ADF_REQUEST_TABLES);
            for (int i = 0; i < tables.length(); i++) {
                JSONObject table = tables.getJSONObject(i);
                tablesMap.put(table.getString(ADFJsonConstants.ADF_REQUEST_NAME), table);
            }

            // Set the activity extended properties
            activity = message.getJSONObject(ADFJsonConstants.ADF_REQUEST_ACTIVITY);
            if (activity == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_ACTIVITY + " not found in ADF"
                        + " request.");
            }
            JSONObject activityProperties = activity.getJSONObject(ADFJsonConstants.ADF_REQUEST_TRANSFORMATION);
            if (activityProperties == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_TRANSFORMATION + " not found"
                        + " in ADF request.");
            }

            JSONObject scheduler = activity.getJSONObject(ADFJsonConstants.ADF_REQUEST_SCHEDULER);
            frequency = scheduler.getString(ADFJsonConstants.ADF_REQUEST_FREQUENCY).toLowerCase() + "s("
                    + scheduler.getInt(ADFJsonConstants.ADF_REQUEST_INTERVAL) + ")";

            activityExtendedProperties = activityProperties.getJSONObject(
                    ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES);
            if (activityExtendedProperties == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES + " not"
                        + " found in ADF request.");
            }

            // should be called after setting activityExtendedProperties
            //proxyUser = getRunAsUser();
            // TODO(yzheng): read from message: "activity" -> "runOnBehalf"
            proxyUser = "ambari-qa";
            // log in the user
            CurrentUser.authenticate(proxyUser);
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

    protected String getClusterName(String linkedServiceName) {
        JSONObject linkedService = linkedServicesMap.get(linkedServiceName);
        if (linkedService == null) {
            return null;
        }

        try {
            return linkedService.getJSONObject(ADFJsonConstants.ADF_REQUEST_PROPERTIES)
                    .getJSONObject(ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES)
                    .getString(ADFJsonConstants.ADF_REQUEST_CLUSTER_NAME);
        } catch (JSONException e) {
            return null;
        }
    }

    protected String getRunAsUser() throws FalconException {
        if (activityExtendedProperties.has(ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER)) {
            String runAsUser = null;
            try {
                runAsUser = activityExtendedProperties.getString(ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER);
            } catch (JSONException e) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER + " not"
                        + " found in ADF request.");
            }

            if (StringUtils.isBlank(runAsUser)) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER + " in"
                        + " ADF request activity extended properties cannot be empty.");
            }
            return runAsUser;
        } else {
            String hadoopLinkedService = getHadoopLinkedService();
            JSONObject linkedService = linkedServicesMap.get(hadoopLinkedService);
            if (linkedService == null) {
                throw new FalconException("JSON object " + hadoopLinkedService + " not"
                        + " found in ADF request.");
            }

            try {
                return linkedService.getJSONObject(ADFJsonConstants.ADF_REQUEST_PROPERTIES)
                        .getJSONObject(ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES)
                        .getString(ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER);
            } catch (JSONException e) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER + " not"
                        + " found in ADF request.");
            }
        }
    }

    protected List<String> getInputTables() {
        List<String> tables = new ArrayList<String>();
        try {
            JSONArray inputs = message.getJSONObject(ADFJsonConstants.ADF_REQUEST_ACTIVITY)
                    .getJSONArray(ADFJsonConstants.ADF_REQUEST_INPUTS);
            for (int i = 0; i < inputs.length(); i++) {
                tables.add(inputs.getJSONObject(i).getString(ADFJsonConstants.ADF_REQUEST_NAME));
            }
        } catch (JSONException e) {
            return null;
        }
        return tables;
    }

    protected List<String> getOutputTables() {
        List<String> tables = new ArrayList<String>();
        try {
            JSONArray outputs = message.getJSONObject(ADFJsonConstants.ADF_REQUEST_ACTIVITY)
                    .getJSONArray(ADFJsonConstants.ADF_REQUEST_OUTPUTS);
            for (int i = 0; i < outputs.length(); i++) {
                tables.add(outputs.getJSONObject(i).getString(ADFJsonConstants.ADF_REQUEST_NAME));
            }
        } catch (JSONException e) {
            return null;
        }
        return tables;
    }

    protected String getADFTablePath(String tableName) throws FalconException {
        JSONObject table = tablesMap.get(tableName);
        if (table == null) {
            throw new FalconException("JSON object " + tableName + " not"
                    + " found in ADF request.");
        }

        try {
            return table.getJSONObject(ADFJsonConstants.ADF_REQUEST_PROPERTIES)
                    .getJSONObject(ADFJsonConstants.ADF_REQUEST_LOCATION)
                    .getJSONObject(ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES)
                    .getString(ADFJsonConstants.ADF_REQUEST_FOLDER_PATH);
        } catch (JSONException e) {
            throw new FalconException("Error when parsing ADF JSON message: " + tableName, e);
        }
    }

    protected String getTableCluster(String tableName) {
        JSONObject table = tablesMap.get(tableName);
        if (table == null) {
            return null;
        }

        try {
            String linkedServiceName = table.getJSONObject(ADFJsonConstants.ADF_REQUEST_PROPERTIES)
                    .getJSONObject(ADFJsonConstants.ADF_REQUEST_LOCATION)
                    .getString(ADFJsonConstants.ADF_REQUEST_LINKED_SERVICE_NAME);
            return getClusterName(linkedServiceName);
        } catch (JSONException e) {
            return null;
        }
    }

    protected boolean activityHasScriptPath() throws FalconException {
        if (JobType.REPLICATION == jobType()) {
            return false;
        }

        return activityExtendedProperties.has(
                ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH);
    }

    protected String getScriptPath() throws FalconException {
        if (!activityHasScriptPath()) {
            throw new FalconException("JSON object does not have object: "
                    + ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH);
        }

        try {
            String scriptPath = activityExtendedProperties.getString(ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH);
            if (StringUtils.isBlank(scriptPath)) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH + " not"
                        + " found or empty in ADF request.");
            }
            return scriptPath;
        } catch (JSONException jsonException) {
            throw new FalconException("Error when parsing ADF JSON object: "
                    + activityExtendedProperties, jsonException);
        }
    }

    protected String getScriptContent() throws FalconException {
        if (activityHasScriptPath()) {
            throw new FalconException("JSON object does not have object: " + ADFJsonConstants.ADF_REQUEST_SCRIPT);
        }
        try {
            String script = activityExtendedProperties.getString(ADFJsonConstants.ADF_REQUEST_SCRIPT);
            if (StringUtils.isBlank(script)) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_SCRIPT + " cannot"
                        + " be empty in ADF request.");
            }
            return script;
        } catch (JSONException jsonException) {
            throw new FalconException("Error when parsing ADF JSON object: "
                    + activityExtendedProperties, jsonException);
        }
    }

    protected String getAdditionalScriptProperties() throws FalconException {
        String[] propertyObjects = JSONObject.getNames(activityExtendedProperties);
        StringBuilder properties = new StringBuilder();
        for (String obj : propertyObjects) {
            if (StringUtils.isBlank(obj) || obj.equalsIgnoreCase(ADFJsonConstants.ADF_REQUEST_SCRIPT)
                    || obj.equalsIgnoreCase(ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER)
                    || obj.equalsIgnoreCase(ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH)) {
                continue;
            }

            String key = obj;
            String value;
            try {
                value = activityExtendedProperties.getString(obj);
            } catch (JSONException jsonException) {
                throw new FalconException("Error when parsing ADF JSON object: "
                        + activityExtendedProperties, jsonException);
            }
            properties.append(PROPERTY_START);
            properties.append(PROPERTY_NAME);
            properties.append(PROPERTY_DOUBLE_QUOTES);
            properties.append(key);
            properties.append(PROPERTY_DOUBLE_QUOTES);
            properties.append(" ");
            properties.append(PROPERTY_VALUE);
            properties.append(PROPERTY_DOUBLE_QUOTES);
            properties.append(value);
            properties.append(PROPERTY_DOUBLE_QUOTES);
            properties.append(PROPERTY_END);
            properties.append(System.lineSeparator());
        }
        return properties.toString();
    }

    protected String getClusterNameToRunProcessOn() throws FalconException {
        return getClusterName(getHadoopLinkedService());
    }

    protected Entity submitAndScheduleJob(String entityType, String msg) {
        Entity entity = jobManager.submitJob(entityType, msg);
        jobManager.scheduleJob(entityType, jobEntityName());
        return entity;
    }

    private String getHadoopLinkedService() throws FalconException {
        String hadoopLinkedService;
        try {
            hadoopLinkedService = activity.getString(ADFJsonConstants.ADF_REQUEST_LINKED_SERVICE_NAME);
        } catch (JSONException jsonException) {
            throw new FalconException("Error when parsing ADF JSON object: "
                    + activity, jsonException);
        }

        if (StringUtils.isBlank(hadoopLinkedService)) {
            throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_LINKED_SERVICE_NAME
                    + " in the activity cannot be empty in ADF request.");
        }
        return hadoopLinkedService;
    }

    protected static class ADFJobManager extends AbstractSchedulableEntityManager {
        public Entity submitJob(String entityType, String msg) {
            InputStream stream = IOUtils.toInputStream(msg);
            LOG.info("to stream");
            try {
                Entity entity = submitInternal(stream, entityType);
                LOG.info("submitted entity: " + entity.getName());
                return entity;
            } catch (Exception e) {
                LOG.info(e.toString());
            }
            return null;
        }

        public void scheduleJob(String entityType, String entityName) {
            try {
                scheduleInternal(entityType, entityName, null);
                LOG.info("scheduled entity: " + entityName);
            } catch (Exception e) {
                LOG.info(e.toString());
            }
        }
    }
}
