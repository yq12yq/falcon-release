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

import org.apache.falcon.hive.util.DBReplicationStatus;
import org.apache.falcon.hive.util.ReplicationStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bvellanki on 2/3/15.
 */
@Test(groups = {"exhaustive"})
public class DBReplicationStatusTest {

    private Map<String, ReplicationStatus> tableStatuses = new HashMap<String, ReplicationStatus>();
    private ReplicationStatus dbReplicationStatus;

    public DBReplicationStatusTest() {}


    @BeforeClass
    public void prepare() throws Exception {
        dbReplicationStatus = new ReplicationStatus("source", "target", "jobname",
                "default1", null, ReplicationStatus.Status.SUCCESS, 0L);
        ReplicationStatus tableStatus = new ReplicationStatus("source", "target", "jobname",
                "default1", "table1", ReplicationStatus.Status.SUCCESS, 0L);
        tableStatuses.put("table1", tableStatus);
    }

    public void DBReplicationStatusSerDeTest() throws Exception {
        DBReplicationStatus replicationStatus = new DBReplicationStatus();
        replicationStatus.setDbReplicationStatus(dbReplicationStatus);
        replicationStatus.setTableStatuses(tableStatuses);
        DBReplicationStatus test = new DBReplicationStatus(replicationStatus.toJsonString());
        Assert.assertEquals("default1", test.getDbReplicationStatus().getDatabase());

    }

    public void ReplicationStatusSerDeTest() throws Exception {
        String expected = "{\n    \"sourceUri\": \"source\",\n"
                + "    \"targetUri\": \"target\",\n    \"jobName\": \"jobname\",\n"
                + "    \"database\": \"default1\",\n    \"table\": \"table1\",\n"
                + "    \"status\": \"SUCCESS\",\n    \"eventId\": 0\n}";
        String actual = tableStatuses.get("table1").toJsonString();
        Assert.assertEquals(actual, expected);
        ReplicationStatus status = new ReplicationStatus(actual);
        Assert.assertEquals(status.getTable(), "table1");
    }

}
