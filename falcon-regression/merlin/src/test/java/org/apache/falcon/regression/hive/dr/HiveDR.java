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

import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.RecipeMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.supportClasses.NotifyingAssert;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HiveAssert;
import org.apache.falcon.regression.core.util.HiveUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

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
    private final OozieClient clusterOC2 = serverOC.get(1);
    private final String baseTestHDFSDir = baseHDFSDir + "/HiveDR/";
    private HCatClient clusterHC;
    private HCatClient clusterHC2;
    RecipeMerlin recipeMerlin;
    Connection connection;
    Connection connection2;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        clusterHC = cluster.getClusterHelper().getHCatClient();
        clusterHC2 = cluster2.getClusterHelper().getHCatClient();
        bundles[0] = BundleUtil.readHCatBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[1] = new Bundle(bundles[0], cluster2);
        bundles[0].generateUniqueBundle();
        bundles[1].generateUniqueBundle();
        final ClusterMerlin srcCluster = bundles[0].getClusterElement();
        final ClusterMerlin tgtCluster = bundles[1].getClusterElement();
        Bundle.submitCluster(bundles[1]);

        recipeMerlin = RecipeMerlin.readFromDir("HiveDrRecipe",
            FalconCLI.RecipeOperation.HIVE_DISASTER_RECOVERY)
            .withRecipeCluster(tgtCluster);
        recipeMerlin.withSourceCluster(srcCluster)
            .withTargetCluster(tgtCluster)
            .withFrequency(new Frequency("5", Frequency.TimeUnit.minutes))
            .withValidity(TimeUtil.getTimeWrtSystemTime(-5), TimeUtil.getTimeWrtSystemTime(5));
        recipeMerlin.setUniqueName(this.getClass().getSimpleName());

        connection = cluster.getClusterHelper().getHiveJdbcConnection();
        HiveUtil.runSql(connection, "drop database if exists hdr_sdb1 cascade");
        connection2 = cluster2.getClusterHelper().getHiveJdbcConnection();
        HiveUtil.runSql(connection2, "drop database if exists hdr_sdb1 cascade");
    }

    @Test
    public void recipeSubmission() throws Exception {
        recipeMerlin.withSourceDb("hdr_sdb1").withSourceTable("global_store_sales")
            .withTargetDb("hdr_sdb1").withTargetTable("global_store_sales");
        final List<String> command = recipeMerlin.getSubmissionCommand();

        HiveUtil.runSql(connection, "create database hdr_sdb1");
        HiveUtil.runSql(connection, "use hdr_sdb1");
        HiveUtil.runSql(connection, "create table global_store_sales "
            + "(customer_id string, item_id string, quantity float, price float, time timestamp) "
            + "partitioned by (country string)");

        HiveUtil.runSql(connection2, "create database hdr_sdb1");
        HiveUtil.runSql(connection2, "use hdr_sdb1");
        HiveObjectCreator.bootstrapCopy(connection, clusterFS, "global_store_sales",
            connection2, clusterFS2, "global_store_sales");

        HiveUtil.runSql(connection,
            "insert into table global_store_sales partition (country = 'us') values"
                + "('c1', 'i1', '1', '1', '2001-01-01 01:01:01')");
        HiveUtil.runSql(connection,
            "insert into table global_store_sales partition (country = 'uk') values"
                + "('c2', 'i2', '2', '2', '2001-01-01 01:01:02')");
        HiveUtil.runSql(connection, "select * from global_store_sales");

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(clusterOC2, recipeMerlin.getName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable("hdr_sdb1", "global_store_sales"),
            cluster2, clusterHC2.getTable("hdr_sdb1", "global_store_sales"), new NotifyingAssert(true)
        ).assertAll();
    }

    @Test
    public void dataGeneration() throws Exception {
        HiveUtil.runSql(connection, "create database hdr_sdb1");
        HiveUtil.runSql(connection, "use hdr_sdb1");
        HiveObjectCreator.createVanillaTable(connection);
        HiveObjectCreator.createSerDeTable(connection);
        HiveObjectCreator.createPartitionedTable(connection);
        HiveObjectCreator.createExternalTable(connection, clusterFS,
            baseTestHDFSDir + "click_data/");

        HiveUtil.runSql(connection2, "create database hdr_sdb1");
        HiveUtil.runSql(connection2, "use hdr_sdb1");
        HiveObjectCreator.createVanillaTable(connection2);
        HiveObjectCreator.createSerDeTable(connection2);
        HiveObjectCreator.createPartitionedTable(connection2);
        HiveObjectCreator.createExternalTable(connection2, clusterFS2,
            baseTestHDFSDir + "click_data/");

        HiveAssert.assertDbEqual(cluster, clusterHC.getDatabase("hdr_sdb1"),
            cluster2, clusterHC2.getDatabase("hdr_sdb1"), new NotifyingAssert(true)
        ).assertAll();

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable("hdr_sdb1", "click_data"),
            cluster2, clusterHC2.getTable("hdr_sdb1", "click_data"), new NotifyingAssert(true)
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

    /**
     * Test creates a table on first cluster using static partitioning. Then it creates the same
     * table on the second cluster using dynamic partitioning. Finally it checks the equality of
     * these tables.
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void dynamicPartitionsTest() throws SQLException, IOException {
        HiveUtil.runSql(connection, "create database hdr_sdb1");
        HiveUtil.runSql(connection, "use hdr_sdb1");
        //create table with static partitions on first cluster
        HiveObjectCreator.createPartitionedTable(connection, false);

        HiveUtil.runSql(connection2, "create database hdr_sdb1");
        HiveUtil.runSql(connection2, "use hdr_sdb1");
        //create table with dynamic partitions on second cluster
        HiveObjectCreator.createPartitionedTable(connection2, true);

        //check that both tables are equal
        HiveAssert.assertTableEqual(
            cluster, clusterHC.getTable("hdr_sdb1", "global_store_sales"),
            cluster2, clusterHC2.getTable("hdr_sdb1", "global_store_sales"), new SoftAssert()
        ).assertAll();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        try {
            prism.getProcessHelper().deleteByName(recipeMerlin.getName(), null);
        } catch (Exception e) {
            LOGGER.info("Deletion of process: " + recipeMerlin.getName() + " failed with " +
                "exception: " +e);
        }
        removeBundles();
        cleanTestDirs();
    }

}
