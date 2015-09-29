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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.ADFService.util.ADFJsonConstants;
import org.apache.falcon.ADFService.util.FSUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;


/**
 * Azure ADF Hive Job.
 */
public class ADFHiveJob extends ADFJob {
    private static final String HIVE_PROCESS_TEMPLATE_FILE = "hive-process.xml";
    private static final String HIVE_PROCESS_SCRIPTS_PATH = TEMPLATE_PATH_PREFIX + "/scripts";
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
            hiveScriptPath = getScriptPath();
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

    private String getScriptPath() throws FalconException {
        String scriptPath;
        try {
            JSONObject scriptObject = activityExtendedProperties.getJSONObject(ADFJsonConstants.ADF_REQUEST_SCRIPT);
            if (scriptObject == null) {
                // get the script Path
                scriptPath = activityExtendedProperties.getString(ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH);
                if (StringUtils.isBlank(scriptPath)) {
                    throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH + " not"
                            + " found or empty in ADF request.");
                }
            } else {
                String script = activityExtendedProperties.getString(ADFJsonConstants.ADF_REQUEST_SCRIPT);
                if (StringUtils.isBlank(script)) {
                    throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_SCRIPT + " cannot"
                            + " be empty in ADF request.");
                }
                // write script to file and set the scriptPath
                scriptPath = createScriptFile(script);
            }

        } catch (JSONException jsonException) {
            throw new FalconException("Error when parsing ADF JSON message: " + message, jsonException);
        }
        return scriptPath;
    }

    private String createScriptFile(String scriptContent) throws FalconException {
        // path is unique as job name is always unique
        /* ToDo - what to do with year, month and day passed? */
        final Path path = new Path(HIVE_PROCESS_SCRIPTS_PATH, jobEntityName() + HIVE_SCRIPT_EXTENSION);
        OutputStream out = null;
        try {
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(path.toUri());
            HadoopClientFactory.mkdirsWithDefaultPerms(fs, path);
            out = fs.create(path);
            out.write(scriptContent.getBytes());
        } catch (IOException e) {
            throw new FalconException("Error preparing script file: " + path, e);
        } finally {
            IOUtils.closeQuietly(out);
        }
        return path.toString();
    }

    private TableFeed getInputTableFeed() throws FalconException {
        return getTableFeed(INPUT_FEED_PREFIX + jobEntityName(), getInputTables().get(0));
    }

    private TableFeed getOutputTableFeed() throws FalconException {
        return getTableFeed(OUTPUT_FEED_PREFIX + jobEntityName(), getOutputTables().get(0));
    }

    private TableFeed getTableFeed(String feedName, String tableName) throws FalconException {
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

    private JSONObject getTableExtendedProperties(String tableName) throws FalconException {
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
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_LOCATION
                        + " not found in ADF request.");
            }
            return tableExtendedProperties;
        } catch (JSONException e) {
            throw new FalconException("Error when parsing ADF JSON message: " + table, e);
        }
    }
}
