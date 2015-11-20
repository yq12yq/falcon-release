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
 * Falcon process event to interface with Atlas Service.
 */
public class FalconProcessEvent extends FalconEvent {
    private List<String> clusters;
    private List<FalconFeedEvent> inputs;
    private List<FalconFeedEvent> outputs;
    private String processEntity;
    private List<String> pipelines;
    private Map<String, String> wfProperties;
    private Map<String, String> counters;

    public FalconProcessEvent(String doAsUser, UserGroupInformation ugi, OPERATION falconOperation,
                              String name, String type, long timestamp, Map<String, String> tags,
                              List<FalconFeedEvent> inputs, List<FalconFeedEvent> outputs, List<String> clusters,
                              String processEntity, List<String> pipelines, Map<String, String> wfProperties,
                              Map<String, String> counters) {
        this(doAsUser, ugi, falconOperation, name, type, timestamp, tags, inputs,
                outputs, clusters, processEntity, pipelines, wfProperties, counters, true);    }

    public FalconProcessEvent(String doAsUser, UserGroupInformation ugi, OPERATION falconOperation,
                              String name, String type, long timestamp, Map<String, String> tags,
                              List<FalconFeedEvent> inputs, List<FalconFeedEvent> outputs, List<String> clusters,
                              String processEntity, List<String> pipelines,
                              Map<String, String> wfProperties,
                              Map<String, String> counters, boolean sync) {
        super(doAsUser, ugi, falconOperation, name, type, timestamp, tags, sync);
        this.clusters = clusters;
        this.processEntity = processEntity;
        this.inputs = inputs;
        this.outputs = outputs;
        this.pipelines = pipelines;
        this.wfProperties = wfProperties;
        this.counters = counters;
    }

    public List<String> getClusters() {
        return clusters;
    }

    public String getProcessEntity() {
        return processEntity;
    }

    public List<FalconFeedEvent> getInputs() {
        return inputs;
    }

    public List<FalconFeedEvent> getOutputs() {
        return outputs;
    }

    public List<String> getPipelines() {
        return pipelines;
    }

    public Map<String, String> getWFProperties() {
        return wfProperties;
    }

    public Map<String, String> getCounters() {
        return counters;
    }
}
