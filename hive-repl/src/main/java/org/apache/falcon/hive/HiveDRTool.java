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

package org.apache.falcon.hive;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.falcon.hive.mapreduce.CopyMapper;
import org.apache.falcon.hive.mapreduce.CopyReducer;
import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.DelimiterUtils;
import org.apache.falcon.hive.util.HiveDRStatusStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ListIterator;

/**
 * DR Tool Driver.
 */
public class HiveDRTool extends Configured implements Tool {
    private FileSystem fs;

    private HiveDROptions inputOptions;
    private DRStatusStore drStore;
    private boolean submitted;

    private static final String DEFAULT_EVENT_STORE_PATH = DRStatusStore.BASE_DEFAULT_STORE_PATH + "/Events";
    private static final FsPermission FS_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
    private static final Logger LOG = LoggerFactory.getLogger(HiveDRTool.class);

    public HiveDRTool() {
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            return -1;
        }

        try {
            init(args);
        } catch (Throwable e) {
            LOG.error("Invalid arguments: ", e);
            System.err.println("Invalid arguments: " + e.getMessage());
            usage();
            return -1;
        }

        try {
            execute();
        } catch (Exception e) {
            System.err.println("Exception encountered " + e.getMessage());
            e.printStackTrace();
            LOG.error("Exception encountered ", e);
            return -1;
        }

        return 0;
    }

    private void init(String[] args) throws Exception {
        LOG.info("Enter init");
        inputOptions = parseOptions(args);
        LOG.info("Input Options: {}", inputOptions);

        fs = FileSystem.get(getConfiguration(inputOptions.getTargetWriteEP()));
        // init DR status store
        drStore = new HiveDRStatusStore(fs);

        // Create base dir to store events
        Path dir = new Path(DEFAULT_EVENT_STORE_PATH);

        if (!fs.exists(dir)) {
            if(!fs.mkdirs(dir)) {
                throw new Exception("Creating directory failed: " + dir);
            }
        }
        LOG.info("Exit init");
    }

    private HiveDROptions parseOptions(String[] args) throws ParseException {
        return HiveDROptions.create(args);
    }

    public Job execute() throws Exception {
        assert inputOptions != null;
        assert getConf() != null;

        ListIterator<ReplicationEvents> events = sourceEvents();
        if(events == null || !events.hasNext()) {
            LOG.info("No events to process");
            return null;
        }

        String identifier = inputOptions.getJobName();
        String inputFilename = persistReplicationEvents(DEFAULT_EVENT_STORE_PATH, identifier, events);

        Job job = null;
        try {
            job = createJob(inputFilename);
            createPartitions(job);

            job.submit();
            submitted = true;
        } finally {
            if (!submitted) {
                cleanup();
            }
        }

        String jobID = job.getJobID().toString();
        job.getConfiguration().set("HIVEDR_JOB_ID", jobID);

        LOG.info("HiveDR job-id: {}", jobID);
        if (inputOptions.shouldBlock() && !job.waitForCompletion(true)) {
            throw new IOException("HiveDR failure: Job " + jobID + " has failed: "
                    + job.getStatus().getFailureInfo());
        }

        return job;
    }

    private Job createJob(String inputFile) throws IOException {
        String jobName = "hive-dr";
        String userChosenName = getConf().get(JobContext.JOB_NAME);
        if (userChosenName != null) {
            jobName += ": " + userChosenName;
        }
        Job job = Job.getInstance(getConf());
        job.setJobName(jobName);

        job.setJarByClass(CopyMapper.class);
        job.setMapperClass(CopyMapper.class);
        job.setReducerClass(CopyReducer.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.getConfiguration().set(JobContext.MAP_SPECULATIVE, "false");
        job.getConfiguration().set(JobContext.NUM_MAPS,
                String.valueOf(inputOptions.getMaxMaps()));

        for(HiveDRArgs args : HiveDRArgs.values()) {
            if(inputOptions.getValue(args)!=null) {
                job.getConfiguration().set(args.getName(),inputOptions.getValue(args));
            }
            else {
                job.getConfiguration().set(args.getName(),"null");
            }
        }

        job.getConfiguration().set("inputPath", inputFile); //Todo change /tmp with getInputPath()

        return job;
    }

    private ListIterator<ReplicationEvents> sourceEvents() throws Exception {
        MetaStoreEventSourcer defaultSourcer = null;
        ListIterator<ReplicationEvents> replicationEventsIter = null;
        try {
            defaultSourcer = new MetaStoreEventSourcer(inputOptions.getSourceMetastoreUri(),
                    inputOptions.getTargetMetastoreUri(), new DefaultPartitioner(drStore), drStore);
            replicationEventsIter = defaultSourcer.sourceEvents(inputOptions);
        } finally {
            if (defaultSourcer != null) {
                defaultSourcer.cleanUp();
            }
        }
        LOG.info("Return sourceEvents");
        return replicationEventsIter;
    }

    private void createPartitions(Job job) throws IOException {
        job.getConfiguration().set(FileInputFormat.INPUT_DIR, job.getConfiguration().get("inputPath"));
        job.getConfiguration().set(FileOutputFormat.OUTDIR,"/apps/dr/dummy");
    }

    public static void main(String args[]) {
        int exitCode;
        try {
            HiveDRTool hiveDRTool = new HiveDRTool();
            exitCode = ToolRunner.run(getDefaultConf(), hiveDRTool, args);
        }
        catch (Exception e) {
            LOG.error("Couldn't complete HiveDR operation: ", e);
            exitCode = -1;
        }

        System.exit(exitCode);
    }

    private static Configuration getDefaultConf() {
        return new Configuration();
    }

    private void cleanup() {
    }

    /* TODO : MR should delete the file in case of success or failure of map job */
    private String persistReplicationEvents(String dir, String filename,
                                            ListIterator<ReplicationEvents> eventsList) throws Exception {
        OutputStream out = null;
        Path filePath = new Path(getFilename(dir, filename));

        try {
            out = FileSystem.create(fs, filePath, FS_PERMISSION);
            while (eventsList.hasNext()) {
                ReplicationEvents events = eventsList.next();
                String dbName = events.getDbName();
                String tableName = events.getTableName();
                ListIterator<Command> exportCmds = events.getExportCommands();
                ListIterator<Command> importCmds = events.getImportCommands();

                if (dbName != null) {
                    out.write(dbName.getBytes());
                }
                out.write(DelimiterUtils.getEscapedFieldDelim().getBytes());
                if (tableName != null) {
                    out.write(tableName.getBytes());
                }
                out.write(DelimiterUtils.getEscapedFieldDelim().getBytes());
                String exportEventStr;
                if ((exportEventStr = getCmdAsString(exportCmds)) != null) {
                    out.write(exportEventStr.getBytes());
                }
                out.write(DelimiterUtils.getEscapedFieldDelim().getBytes());
                String importEventStr;
                if ((importEventStr = getCmdAsString(importCmds)) != null) {
                    out.write(importEventStr.getBytes());
                }
            }
        } finally {
            IOUtils.closeQuietly(out);
        }
        return filePath.toString();
    }

    private static String getCmdAsString(ListIterator<Command> cmds) throws IOException {
        StringBuilder eventStr = new StringBuilder();

        while (cmds.hasNext()) {
            eventStr.append(ReplicationUtils.serializeCommand(cmds.next()));
            eventStr.append(DelimiterUtils.getEscapedStmtDelim());
        }
        if (eventStr.length() > 0) {
            String s = eventStr.toString();
            s = s.substring(0, s.length() - DelimiterUtils.STMT_DELIM.length());
            return s;
        } else {
            return null;
        }
    }

    private static String getFilename(String dir, String identifier) throws Exception {
        String prefix = identifier + "-" + System.currentTimeMillis();
        return dir + File.separator + prefix + ".txt";
    }

    private static Configuration getConfiguration(final String storageEndpoint) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", storageEndpoint);
        return conf;
    }

/*
    private boolean isSubmitted() {
        return submitted;
    }
*/

    public static void usage() {
        System.out.println("Usage: hivedrtool -option value ....");
    }
}
