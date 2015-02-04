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

package org.apache.falcon.hive.mapreduce;

import org.apache.falcon.hive.util.EventUtils;
import org.apache.falcon.hive.util.ReplicationStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;


public class CopyMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Configuration conf;
    public EventUtils eventUtils;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        eventUtils = new EventUtils(conf);
        eventUtils.initializeFS();
        eventUtils.setupConnection();
        eventUtils.initializeHiveDRStore();
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        List<ReplicationStatus> replicationStatusList = null;
        try {
            System.out.println("Processing Event value:"+value.toString());
            eventUtils.processEvents(value.toString());
            replicationStatusList = eventUtils.getListReplicationStatus();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for(ReplicationStatus rs : replicationStatusList) {
            context.write(new Text(rs.getJobName()), new Text(rs.toString()));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        try {
            eventUtils.closeConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
