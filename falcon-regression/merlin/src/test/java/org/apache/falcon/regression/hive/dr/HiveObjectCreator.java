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

import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.HiveUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Create Hive tables for testing Hive DR. Note that this is not expected to be used out of
 * HiveDR tests.
 */
class HiveObjectCreator {
    private HiveObjectCreator() {
        throw new AssertionError("Instantiating utility class...");
    }

    /**
     * Create an external table
     * @param connection jdbc connection object to use for issuing queries to hive
     * @param fs filesystem object to upload the data
     * @param clickDataLocation location to upload the data to
     * @throws IOException
     * @throws SQLException
     */
    static void createExternalTable(Connection connection, FileSystem fs, String
        clickDataLocation) throws IOException, SQLException {
        final String clickDataPart1 = clickDataLocation + "2001-01-01/";
        final String clickDataPart2 = clickDataLocation + "2001-01-02/";
        fs.mkdirs(new Path(clickDataLocation));
        fs.setPermission(new Path(clickDataLocation), FsPermission.getDirDefault());
        HadoopUtil.writeDataForHive(fs, clickDataPart1,
            new StringBuffer("click1").append((char) 0x01).append("01:01:01"), true);
        HadoopUtil.writeDataForHive(fs, clickDataPart2,
            new StringBuffer("click2").append((char) 0x01).append("02:02:02"), true);
        //clusterFS.setPermission(new Path(clickDataPart2), FsPermission.getFileDefault());
        HiveUtil.runSql(connection, "create external table click_data "
            + "(data string, time string) partitioned by (date string) "
            + "location '" + clickDataLocation + "'");
        HiveUtil.runSql(connection, "alter table click_data add partition "
            + "(date='2001-01-01') location '" + clickDataPart1 + "'");
        HiveUtil.runSql(connection, "alter table click_data add partition "
            + "(date='2001-01-02') location '" + clickDataPart2 + "'");
        HiveUtil.runSql(connection, "select * from click_data");
    }

    /**
     * Create an partitioned table
     * @param connection jdbc connection object to use for issuing queries to hive
     * @throws SQLException
     */
    static void createPartitionedTable(Connection connection) throws SQLException {
        HiveUtil.runSql(connection, "create table global_store_sales "
            + "(customer_id string, item_id string, quantity float, price float, time timestamp) "
            + "partitioned by (country string)");
        HiveUtil.runSql(connection,
            "insert into table global_store_sales partition (country = 'us') values"
                + "('c1', 'i1', '1', '1', '2001-01-01 01:01:01')");
        HiveUtil.runSql(connection,
            "insert into table global_store_sales partition (country = 'uk') values"
                + "('c2', 'i2', '2', '2', '2001-01-01 01:01:02')");
        HiveUtil.runSql(connection, "select * from global_store_sales");
    }

    /**
     * Create an plain old table
     * @param connection jdbc connection object to use for issuing queries to hive
     * @throws SQLException
     */
    static void createVanillaTable(Connection connection) throws SQLException {
        //vanilla table
        HiveUtil.runSql(connection, "create table store_sales "
            + "(customer_id string, item_id string, quantity float, price float, time timestamp)");
        HiveUtil.runSql(connection, "insert into table store_sales values "
            + "('c1', 'i1', '1', '1', '2001-01-01 01:01:01'), "
            + "('c2', 'i2', '2', '2', '2001-01-01 01:01:02')");
        HiveUtil.runSql(connection, "select * from store_sales");
    }
}
