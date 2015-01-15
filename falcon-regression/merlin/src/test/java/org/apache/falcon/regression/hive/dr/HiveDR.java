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

package org.apache.falcon.regression.hive.dr;

import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.supportClasses.NotifyingAssert;
import org.apache.falcon.regression.core.util.HiveAssert;
import org.apache.falcon.regression.core.util.HiveUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.sql.Connection;

/**
 * Hive DR Testing.
 */
public class HiveDR extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(HiveDR.class);
    private final ColoHelper cluster = servers.get(0);
    private final ColoHelper cluster2 = servers.get(1);
    private final FileSystem clusterFS = serverFS.get(0);
    private final FileSystem clusterFS2 = serverFS.get(1);
    private final OozieClient clusterOC = serverOC.get(0);
    private final String baseTestHDFSDir = baseHDFSDir + "/HiveDR/";
    private HCatClient clusterHC;
    private HCatClient clusterHC2;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        clusterHC = cluster.getClusterHelper().getHCatClient();
        clusterHC2 = cluster2.getClusterHelper().getHCatClient();
    }

    @Test
    public void dataGeneration() throws Exception {
        final Connection connection = cluster.getClusterHelper().getHiveJdbcConnection();
        HiveUtil.runSql(connection, "show tables");
        HiveUtil.runSql(connection, "drop database if exists hdr_sdb1 cascade");
        HiveUtil.runSql(connection, "create database hdr_sdb1");
        HiveUtil.runSql(connection, "use hdr_sdb1");
        HiveObjectCreator.createVanillaTable(connection);
        HiveObjectCreator.createPartitionedTable(connection);
        HiveObjectCreator.createExternalTable(connection, clusterFS,
            baseTestHDFSDir + "click_data/");

        final Connection connection2 = cluster2.getClusterHelper().getHiveJdbcConnection();
        HiveUtil.runSql(connection2, "show tables");
        HiveUtil.runSql(connection2, "drop database if exists hdr_tdb1 cascade");
        HiveUtil.runSql(connection2, "create database hdr_tdb1");
        HiveUtil.runSql(connection2, "use hdr_tdb1");
        HiveObjectCreator.createVanillaTable(connection2);
        HiveObjectCreator.createPartitionedTable(connection2);
        HiveObjectCreator.createExternalTable(connection2, clusterFS2,
            baseTestHDFSDir + "click_data/");

        HiveAssert.assertDbEqual(cluster, clusterHC.getDatabase("hdr_sdb1"),
            cluster2, clusterHC2.getDatabase("hdr_tdb1"), new NotifyingAssert(true)
        ).assertAll();

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable("hdr_sdb1", "click_data"),
            cluster2, clusterHC2.getTable("hdr_tdb1", "click_data"), new NotifyingAssert(true)
        ).assertAll();

    }

    @Test
    public void assertionTest() throws Exception {
        HiveAssert.assertTableEqual(
            cluster, clusterHC.getTable("default", "hcatsmoke10546"),
            cluster2, clusterHC2.getTable("default", "hcatsmoke10548"), new SoftAssert()
        ).assertAll();
        HiveAssert.assertDbEqual(cluster, clusterHC.getDatabase("default"), cluster2,
            clusterHC2.getDatabase("default"), new SoftAssert()
        ).assertAll();
    }

}
