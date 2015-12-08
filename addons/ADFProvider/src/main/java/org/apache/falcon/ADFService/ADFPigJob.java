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

import org.apache.falcon.ADFService.util.FSUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure ADF Pig Job.
 */
public class ADFPigJob extends ADFJob {
    private static final Logger LOG = LoggerFactory.getLogger(ADFPigJob.class);
    private static final String PIG_SCRIPT_EXTENSION = ".pig";
    private static final String ENGINE_TYPE = "pig";
    private static final String INPUT_FEED_SUFFIX = "-pig-input-feed";
    private static final String OUTPUT_FEED_SUFFIX = "-pig-output-feed";
    private static final String INPUTNAME = "input";
    private static final String OUTPUTNAME = "output";

    private String pigScriptPath;
    private DataFeed inputDataFeed;
    private DataFeed outputDataFeed;

    public ADFPigJob(String message, String id) throws FalconException {
        super(message, id);
        type = JobType.PIG;

        inputDataFeed = getInputFeed();
        outputDataFeed = getOutputFeed();
        // set the script path
        pigScriptPath = getPigScriptPath();
    }

    @Override
    public void startJob() throws FalconException {
        // submit feeds
        LOG.info("submitting input data feed: {}", inputDataFeed.getName());
        jobManager.submitJob(EntityType.FEED.name(), inputDataFeed.getEntityxml());

        LOG.info("submitting output data feed: {}", outputDataFeed.getName());
        jobManager.submitJob(EntityType.FEED.name(), outputDataFeed.getEntityxml());

        String processRequest = new Process.Builder().withProcessName(jobEntityName()).withFrequency(frequency)
                .withStartTime(startTime).withEndTime(endTime).withClusterName(getClusterNameToRunProcessOn())
                .withInputName(INPUTNAME).withInputFeedName(inputDataFeed.getName())
                .withOutputName(OUTPUTNAME).withOutputFeedName(outputDataFeed.getName())
                .withEngineType(ENGINE_TYPE).withWFPath(pigScriptPath).withAclOwner(proxyUser)
                .withProperties(getAdditionalProperties()).build().getEntityxml();

        LOG.info("submitting/scheduling pig process job: {}", processRequest);
        submitAndScheduleJob(EntityType.PROCESS.name(), processRequest);
        LOG.info("submitted and scheduled pig process job: {}", jobEntityName());
    }

    @Override
    public void cleanup() throws FalconException {
        // Delete the entities. Should be called after the job execution success/failure.
        try {
            // delete the feeds
            jobManager.deleteEntity(EntityType.FEED.name(), inputDataFeed.getName());
            jobManager.deleteEntity(EntityType.FEED.name(), outputDataFeed.getName());

            //delete the process
            jobManager.deleteEntity(EntityType.PROCESS.name(), jobEntityName());
        } catch (FalconException e) {
            LOG.error("Exception while cleanup {}", e);
        }

        try {
            // cleanup script files
            FSUtils.removeDir(new Path(ADFJob.PROCESS_SCRIPTS_PATH, jobEntityName()));
        } catch (FalconException e) {
            LOG.error("Couldn't delete the dirs {}", e);
        }
    }

    private DataFeed getInputFeed() throws FalconException {
        return getFeed(jobEntityName() + INPUT_FEED_SUFFIX, getInputTables().get(0),
                getTableCluster(getInputTables().get(0)));
    }

    private DataFeed getOutputFeed() throws FalconException {
        return getFeed(jobEntityName() + OUTPUT_FEED_SUFFIX, getOutputTables().get(0),
                getTableCluster(getOutputTables().get(0)));
    }

    private DataFeed getFeed(final String feedName, final String tableName,
                             final String clusterName) throws FalconException {
        return new DataFeed.Builder().withFeedName(feedName).withFrequency(frequency)
                .withClusterName(clusterName).withStartTime(startTime).withEndTime(endTime)
                .withAclOwner(proxyUser).withLocationPath(getADFTablePath(tableName)).build();
    }

    private String getPigScriptPath() throws FalconException {
        if (activityHasScriptPath()) {
            return getScriptPath();
        } else {
            String content = getScriptContent();
            // file path is unique as job name is always unique
            final Path dir = new Path(ADFJob.PROCESS_SCRIPTS_PATH, jobEntityName());
            // create dir
            FSUtils.createDir(dir);

            final Path path = new Path(dir, jobEntityName() + PIG_SCRIPT_EXTENSION);
            // create script file
            return FSUtils.createFile(path, content);
        }
    }

}
