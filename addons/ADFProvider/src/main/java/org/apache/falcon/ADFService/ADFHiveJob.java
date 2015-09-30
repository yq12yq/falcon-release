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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.ADFService.util.ADFJsonConstants;
import org.apache.falcon.ADFService.util.FSUtils;
import org.apache.falcon.FalconException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;


/**
 * Azure ADF Hive Job.
 */
public class ADFHiveJob extends ADFJob {
    private static final String HIVE_PROCESS_TEMPLATE_FILE = "hive-process.xml";
    private static final String HIVE_SCRIPT_EXTENSION = ".hql";
    private static final String ENGINE_TYPE = "hive";
    private static final String INPUT_FEED_PREFIX = "hive-input-feed-";
    private static final String OUTPUT_FEED_PREFIX = "hive-output-feed-";
    private String hiveScriptPath;
    private TableFeed inputFeed;
    private TableFeed outputFeed;
    private String clusterName;

    public ADFHiveJob(String message, String id) throws FalconException {
        super(message, id);
        type = JobType.HIVE;

        /* ToDo - Why is cluster anme inside input table? Can we move it in activity proeprties? */
        // get cluster
        String inputTableName = getInputTables().get(0);
        clusterName = getTableCluster(inputTableName);

        inputFeed = getInputTableFeed();
        outputFeed = getOutputTableFeed();

        try {
            // set the script path
            hiveScriptPath = getHiveScriptPath();
        } catch (FalconException e) {
            /* TODO - send the error msg to ADF queue */
        }
    }

    public void submitJob() {
        try {
            String template = FSUtils.readTemplateFile(TEMPLATE_PATH_PREFIX + HIVE_PROCESS_TEMPLATE_FILE);

            String message = template.replace("$processName$", jobEntityName())
                    .replace("$frequency$", frequency)
                    .replace("$startTime$", startTime)
                    .replace("$endTime$", endTime)
                    .replace("$$clusterName$$", clusterName)
                    .replace("$inputFeedName$", inputFeed.getName())
                    .replace("$outputFeedName$", outputFeed.getName())
                    .replace("$engine$", ENGINE_TYPE)
                    .replace("$scriptPath$", hiveScriptPath)
                    .replace("$aclowner$", proxyUser);
        } catch (IOException e) {
            /* TODO - handle */
        }
    }

    private String getHiveScriptPath() throws FalconException {
        if (activityHasScriptPath()) {
            return getScriptPath();
        } else {
            String content = getScriptContent();
            String additionalScriptProperties = getHivePropertiesAsString(getAdditionalScriptProperties());
            return FSUtils.createScriptFile(content, additionalScriptProperties, jobEntityName(),
                    HIVE_SCRIPT_EXTENSION);
        }
    }

    private static String getHivePropertiesAsString(final Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }

        StringBuilder content = new StringBuilder();
        content.append(System.getProperty("line.separator"));
        for(Map.Entry<String, String> propertyEntry : properties.entrySet()) {
            content.append(("set " + propertyEntry.getKey() + " = " + propertyEntry.getValue()));
            content.append(System.getProperty("line.separator"));
        }
        return content.toString();
    }

    private TableFeed getInputTableFeed() throws FalconException {
        return getTableFeed(INPUT_FEED_PREFIX + jobEntityName(), getInputTables().get(0));
    }

    private TableFeed getOutputTableFeed() throws FalconException {
        return getTableFeed(OUTPUT_FEED_PREFIX + jobEntityName(), getOutputTables().get(0));
    }

    private TableFeed getTableFeed(final String feedName, final String tableName) throws FalconException {
        JSONObject tableExtendedProperties = getTableExtendedProperties(tableName);
        String tableFeedName;
        String partitions;

        try {
            tableFeedName = tableExtendedProperties.getString(ADFJsonConstants.ADF_REQUEST_TABLE_NAME);
            if (StringUtils.isBlank(tableFeedName)) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_TABLE_NAME + " cannot"
                        + " be empty in ADF request.");
            }
            partitions = tableExtendedProperties.getString(ADFJsonConstants.ADF_REQUEST_TABLE_PARTITION);
            if (StringUtils.isBlank(tableFeedName)) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_TABLE_PARTITION + " cannot"
                        + " be empty in ADF request.");
            }
        } catch (JSONException e) {
            throw new FalconException("Error when parsing ADF JSON message: " + tableExtendedProperties, e);
        }


        return new TableFeed.Builder().withFeedName(feedName).withFrequency(frequency)
                .withClusterName(clusterName).withStartTime(startTime).withEndTime(endTime).
                withAclOwner(proxyUser).withTableName(tableFeedName).withPartitions(partitions).build();
    }

    private JSONObject getTableExtendedProperties(final String tableName) throws FalconException {
        JSONObject table = tablesMap.get(tableName);
        if (table == null) {
            throw new FalconException("JSON object tables  not found in ADF request.");
        }

        try {
            JSONObject tableProperties = table.getJSONObject(ADFJsonConstants.ADF_REQUEST_PROPERTIES);
            if (tableProperties == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_PROPERTIES
                        + " not found in ADF request.");
            }
            JSONObject tablesLocation = tableProperties.getJSONObject(ADFJsonConstants.ADF_REQUEST_LOCATION);
            if (tablesLocation == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_LOCATION
                        + " not found in ADF request.");
            }

            JSONObject tableExtendedProperties = tablesLocation.getJSONObject(ADFJsonConstants.
                    ADF_REQUEST_EXTENDED_PROPERTIES);
            if (tableExtendedProperties == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES
                        + " not found in ADF request.");
            }
            return tableExtendedProperties;
        } catch (JSONException e) {
            throw new FalconException("Error when parsing ADF JSON message: " + table, e);
        }
    }
}
