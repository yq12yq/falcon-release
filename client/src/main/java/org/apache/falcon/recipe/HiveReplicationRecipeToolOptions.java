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

package org.apache.falcon.recipe;

public enum HiveReplicationRecipeToolOptions {
    REPLICATION_SOURCE_CLUSTER("sourceCluster", "Replication source cluster name"),
    REPLICATION_SOURCE_METASTORE_URI("sourceMetastoreUri", "Source Hive metastore uri"),
    REPLICATION_SOURCE_HS2_URI("sourceHiveServer2Uri", "Source HS2 uri"),
    REPLICATION_SOURCE_SERVICE_PRINCIPAL("sourceServicePrincipal", "Source principal"),
    REPLICATION_SOURCE_DATABASE("sourceDatabase", "List of databases to replicate"),
    REPLICATION_SOURCE_TABLE("sourceTable", "List of tables to replicate"),
    REPLICATION_SOURCE_STAGING_PATH("sourceStagingPath", "Location of source staging path"),
    REPLICATION_SOURCE_NN("sourceStagingPath", "Source name node"),
    REPLICATION_SOURCE_RM("sourceStagingPath", "Source resource manager"),

    REPLICATION_TARGET_CLUSTER("targetCluster", "Replication target cluster name"),
    REPLICATION_TARGET_METASTORE_URI("targetMetastoreUri", "Target Hive metastore uri"),
    REPLICATION_TARGET_HS2_URI("targetHiveServer2Uri", "Target HS2 uri"),
    REPLICATION_TARGET_SERVICE_PRINCIPAL("targetServicePrincipal", "Target principal"),
    REPLICATION_TARGET_DATABASE("targetDatabase", "List of databases on target after replication"),
    REPLICATION_TARGET_TABLE("targetTable", "List of tables on target after replication"),
    REPLICATION_TARGET_STAGING_PATH("targetStagingPath", "Location of target staging path"),
    REPLICATION_TARGET_NN("targetNN", "Target name node"),
    REPLICATION_TARGET_RM("targetRM", "Target resource manager"),

    REPLICATION_MAX_EVENTS("maxEvents", "Maximum events to replicate"),
    REPLICATION_MAX_MAPS("maxMaps", "Maximum number of maps used during replication"),
    REPLICATION_MAP_BANDWIDTH_IN_MB("mapBandwidth", "Bandwidth in MB/s used by each mapper during replication"),
    HIVE_DR_JOB_NAME("drJobName", "Unique hive DR job name", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    HiveReplicationRecipeToolOptions(String name, String description) {
        this(name, description, true);
    }

    HiveReplicationRecipeToolOptions(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return description;
    }

    public boolean isRequired() {
        return isRequired;
    }

    @Override
    public String toString() {
        return getName();
    }
}
