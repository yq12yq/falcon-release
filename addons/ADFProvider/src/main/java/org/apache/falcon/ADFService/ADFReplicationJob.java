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

import java.net.URISyntaxException;

import org.apache.falcon.ADFService.util.FSUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.EntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure ADF Replication Job (hive/hdfs to Azure blobs).
 */
public class ADFReplicationJob extends ADFJob {

    private static final Logger LOG = LoggerFactory.getLogger(ADFReplicationJob.class);

    public static final String TEMPLATE_REPLICATION_FEED = "replicate-feed.xml";
    public static final String REPLICATION_TARGET_CLUSTER = "adf-replication-target-cluster";

    public ADFReplicationJob(String message, String id) throws FalconException {
        super(message, id);
        type = JobType.REPLICATION;
    }

    @Override
    public void startJob() throws FalconException {
        try {
            String template = FSUtils.readHDFSFile(TEMPLATE_PATH_PREFIX, TEMPLATE_REPLICATION_FEED);
            LOG.info("template: " + template);
            String inputTableName = getInputTables().get(0);
            String outputTableName = getOutputTables().get(0);

            LOG.info("input table: " + inputTableName);
            LOG.info("output table: " + outputTableName);
            LOG.info("feed name: " + jobEntityName());
            LOG.info("start time: " + startTime);
            LOG.info("cluster source: " + getTableCluster(inputTableName));
            LOG.info("source location: " + getADFTablePath(inputTableName));
            LOG.info("target location: " + getADFTablePath(outputTableName));
            String message = template.replace("$feedName$", jobEntityName())
                    .replace("$frequency$", frequency)
                    .replace("$startTime$", startTime)
                    .replace("$endTime$", endTime)
                    .replace("$clusterSource$", getTableCluster(inputTableName))
                    .replace("$clusterTarget$", REPLICATION_TARGET_CLUSTER)
                    .replace("$sourceLocation$", getADFTablePath(inputTableName))
                    .replace("$targetLocation$", getADFTablePath(outputTableName))
                    .replace("$properties$", getAdditionalProperties());

            LOG.info("submitting/scheduling job: " + message);
            submitAndScheduleJob(EntityType.FEED.name(), message);
            LOG.info("submitted and scheduled job");
        } catch (URISyntaxException e) {
            LOG.info(e.toString());
        }

    }

    @Override
    public void cleanup() throws FalconException {
        // Delete the entities. Should be called after the job execution success/failure.
        try {
            // delete the cluster
            jobManager.deleteEntity(EntityType.CLUSTER.name(), ADFReplicationJob.REPLICATION_TARGET_CLUSTER);
            // delete the replication feed
            jobManager.deleteEntity(EntityType.FEED.name(), jobEntityName());
        } catch (FalconException e) {
            LOG.error("Exception while cleanup {}", e);
        }
    }
}
