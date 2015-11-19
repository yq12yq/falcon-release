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

package org.apache.falcon.atlas.event;

import org.apache.hadoop.security.UserGroupInformation;

import java.util.List;
import java.util.Map;

/**
 * Falcon feed event to interface with Atlas Service.
 */
public class FalconFeedEvent extends FalconEvent {
    private String srcCluster;
    private String targetCluster;
    private String dataSource;
    private List<String> groups;
    private Map<String, String> counters;
    private List<String> paths;

    public FalconFeedEvent(String doAsUser, UserGroupInformation ugi, OPERATION falconOperation,
                           String name, String type, long timestamp, Map<String, String> tags, String dataSource,
                           String srcCluster, String targetCluster, List<String> groups, List<String> paths,
                           Map<String, String> counters) {
        this(doAsUser, ugi, falconOperation, name, type, timestamp, tags, dataSource, srcCluster, targetCluster,
                groups, paths,
             counters, true);
    }

    public FalconFeedEvent(String doAsUser, UserGroupInformation ugi, OPERATION falconOperation,
                    String name, String type, long timestamp, Map<String, String> tags, String dataSource,
                    String srcCluster, String targetCluster, List<String> groups, List<String> paths,
                    Map<String, String> counters, boolean sync) {
        super(doAsUser, ugi, falconOperation, name, type, timestamp, tags, sync);
        this.srcCluster = srcCluster;
        this.targetCluster = targetCluster;
        this.dataSource = dataSource;
        this.groups = groups;
        this.paths = paths;
        this.counters = counters;
    }

    public String getSourceCluster() {
        return srcCluster;
    }
    public String getTargetCluster() {
        return targetCluster;
    }

    public String getDataSource() {
        return dataSource;
    }

    public List<String> getPaths() {
        return paths;
    }

    public List<String> getGroups() {
        return groups;
    }

    public Map<String, String> getCounters() {
        return counters;
    }
}
