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
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure ADF Hive Job.
 */
public class ADFHiveJob extends ADFJob {
    private static final Logger LOG = LoggerFactory.getLogger(ADFHiveJob.class);
    private static final String HIVE_SCRIPT_EXTENSION = ".hql";
    private static final String ENGINE_TYPE = "hive";
    private static final String INPUT_FEED_SUFFIX = "-hive-input-feed";
    private static final String OUTPUT_FEED_SUFFIX = "-hive-output-feed";
    private static final String INPUTNAME = "inputtable";
    private static final String OUTPUTNAME = "outputtable";

    private String hiveScriptPath;
    private TableFeed inputFeed;
    private TableFeed outputFeed;

    public ADFHiveJob(String message, String id) throws FalconException {
        super(message, id);
        type = JobType.HIVE;

        inputFeed = getInputTableFeed();
        outputFeed = getOutputTableFeed();
        // set the script path
        hiveScriptPath = getHiveScriptPath();
    }

    @Override
    public void submitJob() throws FalconException {
        String request = new Process.Builder().withProcessName(jobEntityName()).withFrequency(frequency)
                .withStartTime(startTime).withEndTime(endTime).withClusterName(getClusterNameToRunProcessOn())
                .withInputName(INPUTNAME).withInputFeedName(inputFeed.getName())
                .withOutputName(OUTPUTNAME).withOutputFeedName(outputFeed.getName())
                .withEngineType(ENGINE_TYPE).withWFPath(hiveScriptPath).withAclOwner(proxyUser)
                .withProperties(getAdditionalScriptProperties()).build().getEntityxml();
        /* To Remove */
        LOG.info("Entity: {}", request);
        LOG.info("Input: {}", inputFeed.getEntityxml());
        LOG.info("Output: {}", outputFeed.getEntityxml());
    }

    private String getHiveScriptPath() throws FalconException {
        if (activityHasScriptPath()) {
            return getScriptPath();
        } else {
            String content = getScriptContent();
            // file path is unique as job name is always unique
            final Path path = new Path(ADFJob.PROCESS_SCRIPTS_PATH, jobEntityName() + HIVE_SCRIPT_EXTENSION);
            return FSUtils.createScriptFile(path, content);
        }
    }

    private TableFeed getInputTableFeed() throws FalconException {
        return getTableFeed(jobEntityName() + INPUT_FEED_SUFFIX, getInputTables().get(0),
                getTableCluster(getInputTables().get(0)));
    }

    private TableFeed getOutputTableFeed() throws FalconException {
        return getTableFeed(jobEntityName() + OUTPUT_FEED_SUFFIX, getOutputTables().get(0),
                getTableCluster(getOutputTables().get(0)));
    }

    private TableFeed getTableFeed(final String feedName, final String tableName,
                                   final String clusterName) throws FalconException {
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
