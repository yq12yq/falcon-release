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

import java.util.Arrays;
import java.util.List;

public class EventFilter {
    private final String hiveMetaStoreUri;
    private final List<String> databaseList;
    private final List<String> tableList;

    public EventFilter(String hiveMetaStoreUri, String databaseListAsCSV, String tableListAsCSV) {
        this(hiveMetaStoreUri, databaseListAsCSV.split(","), tableListAsCSV.split(","));
    }

    public EventFilter(String hiveMetaStoreUri, String[] databaseList, String[] tableList) {
        this(hiveMetaStoreUri, Arrays.asList(databaseList), Arrays.asList(tableList));
    }

    public EventFilter(String hiveMetaStoreUri, List<String> databaseList,
                       List<String> tableList) {
        this.hiveMetaStoreUri = hiveMetaStoreUri;
        this.databaseList = databaseList;
        this.tableList = tableList;
    }

    public String getHiveMetaStoreUri() {
        return hiveMetaStoreUri;
    }

    public List<String> getDatabaseList() {
        return databaseList;
    }

    public List<String> getTableList() {
        return tableList;
    }
}
