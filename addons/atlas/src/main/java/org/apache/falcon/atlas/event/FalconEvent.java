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

import java.util.Map;

/**
 * Falcon event to interface with Atlas Service.
 */
public abstract class FalconEvent {
    protected String user;
    protected UserGroupInformation ugi;
    protected OPERATION operation;

    protected String name;
    protected String type;
    protected long timestamp;
    protected Map<String, String> tags;
    protected boolean sync;

    public FalconEvent(String doAsUser, UserGroupInformation ugi, OPERATION falconOperation,
                       String name, String type, long timestamp, Map<String, String> tags, boolean sync) {
        this.user = doAsUser;
        this.ugi = ugi;
        this.operation = falconOperation;
        this.name = name;
        this.type = type;
        this.timestamp = timestamp;
        this.tags = tags;
        this.sync = sync;
    }

    public enum OPERATION {
        // Does support update? Else remove and add
        ADD_CLUSTER_ENTITY,
//        UPDATE_CLUSTER_ENTITY,
//        REMOVE_CLUSTER_ENTITY,
//        ADD_FEED_ENTITY,
//        UPDATE_FEED_ENTITY,
//        REMOVE_FEED_ENTITY,
//        ADD_PROCESS_ENTITY,
//        UPDATE_PROCESS_ENTITY,
//        REMOVE_PROCESS_ENTITY,
        ADD_DATASOURCE_ENTITY,
        ADD_GENERATED_FEED_INSTANCE,
        ADD_REPLICATED_FEED_INSTANCE,
        ADD_EVICTED_FEED_INSTANCE,
        ADD_PROCESS_INSTANCE,
    }

    public String getUser() {
        return user;
    }

    public UserGroupInformation getUgi() {
        return ugi;
    }

    public OPERATION getOperation() {
        return operation;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public boolean getSyncConfig() {
        return sync;
    }
}
