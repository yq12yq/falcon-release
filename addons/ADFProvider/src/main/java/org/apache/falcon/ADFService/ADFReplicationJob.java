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

import com.sun.jersey.api.client.ClientResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.falcon.ADFService.util.FSUtils;
import org.apache.falcon.FalconException;

/**
 * Azure ADF Replication Job (hive/hdfs to Azure blobs)
 */
public class ADFReplicationJob extends ADFJob {

    public static String TEMPLATE_REPLIACATION_FEED = "replicate-feed.xml";
    public static String REPLICATION_TARGET_CLUSTER = "adf-replication-target-cluster";

    public ADFReplicationJob(String message, String id) throws FalconException {
        super(message, id);
        type = JobType.REPLICATION;
    }

    public void submitJob() {
        try {
            String template = FSUtils.readTemplateFile(HDFS_URL_PORT,
                    TEMPLATE_PATH_PREFIX + TEMPLATE_REPLIACATION_FEED);
            String inputTableName = getInputTables().get(0);
            String outputTableName = getOutputTables().get(0);
            String message = template.replace("$feedName$", jobEntityName())
                    .replace("$frequency$", frequency)
                    .replace("$startTime$", startTime)
                    .replace("$endTime$", endTime)
                    .replace("$clusterSource$", getTableCluster(inputTableName))
                    .replace("$clusterTarget$", REPLICATION_TARGET_CLUSTER)
                    .replace("$sourceLocation$", getADFTablePath(inputTableName))
                    .replace("$targetLocation$", getADFTablePath(outputTableName));

            ClientResponse clientResponse = submitAndScheduleJob("feed", message);
        } catch (IOException e) {
            /* TODO - handle */
        } catch (URISyntaxException e) {
            /* TODO - handle */
        }

    }
}
