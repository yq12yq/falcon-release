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

import java.io.IOException;

public class TableFeed {
    private static String TABLE_FEED_TEMPLATE_FILE = "table-feed.xml";
    private static String TABLE_PARTITION_SEPARATOR = "#";

    private String feedName;
    private String clusterName;
    private String frequency;
    private String startTime, endTime;
    private String tableName;
    private String aclOwner;
//    public List<String> partitions;
    private String partitions;

    public TableFeed(String feedName, String frequency, String clusterName,
                     String startTime, String endTime, String aclOwner,
                     String tableName, String partitions) {
        this.feedName = feedName;
        this.clusterName = clusterName;
        this.frequency = frequency;
        this.startTime = startTime;
        this.endTime = endTime;
        this.tableName = tableName;
        this.aclOwner = aclOwner;
        // ToDO - More than 1 partition allowed? Why inisde []?
//        this.partitions = Arrays.asList(partitions.split("[;]"));
        this.partitions = partitions;
    }

    private String getTable() {
        /* TODO - handle multiple partitions if allowed */
        return tableName + TABLE_PARTITION_SEPARATOR + partitions;
    }

    public String getEntityxml() throws FalconException {
        try {
            String template = FSUtils.readTemplateFile(ADFJob.TEMPLATE_PATH_PREFIX
                    + TABLE_FEED_TEMPLATE_FILE);
            String message = template.replace("$feedName$", feedName)
                    .replace("$frequency$", frequency)
                    .replace("$startTime$", startTime)
                    .replace("$endTime$", endTime)
                    .replace("$cluster$", clusterName)
                    .replace("$table$", getTable())
                    .replace("$aclowner$", aclOwner);
            return message;
        } catch (IOException e) {
            throw new FalconException("Error when generating entity xml for table feed", e);
        }
    }

    public String getName() {
        return feedName;
    }

}
