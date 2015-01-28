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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.api.HCatDatabase;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatException;

import java.io.IOException;
import java.util.Properties;

public class HiveReplicationRecipeTool implements Recipe {

    @Override
    public void validate(final Properties recipeProperties)  throws Exception {
        for (HiveReplicationRecipeToolOptions option : HiveReplicationRecipeToolOptions.values()) {
            if (recipeProperties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException("Missing argument: " + option.getName());
            }
        }

        // Validate if DB exists - source and target
        HCatClient sourceMetastoreClient = getHiveMetaStoreClient(recipeProperties.getProperty
                (HiveReplicationRecipeToolOptions.REPLICATION_SOURCE_METASTORE_URI.getName()));

        String sourceDbList = recipeProperties.getProperty(recipeProperties.getProperty(HiveReplicationRecipeToolOptions
                .REPLICATION_SOURCE_DATABASE.getName()));
        String sourceTableList = recipeProperties.getProperty(recipeProperties.getProperty
                (HiveReplicationRecipeToolOptions.REPLICATION_SOURCE_TABLE.getName()));

        String[] srcDbs = sourceDbList.split(",");
        for(String db : srcDbs) {
            if (!dbExists(sourceMetastoreClient, db)) {
                throw new Exception("Database " + db + " doesn't exist on source cluster");
            }
        }

        String[] srcTables =  sourceTableList.split(",");
        if (srcTables.length > 0) {
            for(String table : srcTables) {
                if (!tableExists(sourceMetastoreClient, srcDbs[0], table)) {
                    throw new Exception("Table " + table + " doesn't exist on source cluster");
                }
            }
        }

        HCatClient targetMetastoreClient = getHiveMetaStoreClient(recipeProperties.getProperty
                (HiveReplicationRecipeToolOptions.REPLICATION_TARGET_METASTORE_URI.getName()));

        String trgDbList = recipeProperties.getProperty(recipeProperties.getProperty(HiveReplicationRecipeToolOptions
                .REPLICATION_TARGET_DATABASE.getName()));

        String[] tgrDbs = trgDbList.split(",");
        for(String db : tgrDbs) {
            if (!dbExists(targetMetastoreClient, db)) {
                throw new Exception("Database " + db + " doesn't exist on target cluster");
            }
        }
    }

    @Override
    public Properties getAdditionalSystemProperties(final Properties recipeProperties) {
        Properties additionalProperties = new Properties();
        String recipeName = recipeProperties.getProperty(RecipeToolOptions.RECIPE_NAME.getName());
        // Add recipe name as Hive DR job
        additionalProperties.put(HiveReplicationRecipeToolOptions.HIVE_DR_JOB_NAME, recipeName);
        return additionalProperties;
    }

    private HCatClient getHiveMetaStoreClient(String metastoreUri) throws Exception {
        try {
            HiveConf hcatConf = createHiveConf(new Configuration(false), metastoreUri);
            return HCatClient.create(hcatConf);
        } catch (HCatException e) {
            throw new Exception("Exception creating HCatClient: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new Exception("Exception creating HCatClient: " + e.getMessage(), e);
        }
    }

    private static HiveConf createHiveConf(Configuration conf,
                                           String metastoreUrl) throws IOException {
        HiveConf hcatConf = new HiveConf(conf, HiveConf.class);

        hcatConf.set("hive.metastore.local", "false");
        hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUrl);
        hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
                HCatSemanticAnalyzer.class.getName());
        hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

        hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        return hcatConf;
    }

    private static boolean tableExists(HCatClient client, final String database, final String tableName)
            throws Exception {
        try {
            HCatTable table = client.getTable(database, tableName);
            return table != null;
        } catch (HCatException e) {
            throw new Exception("Exception checking if the table exists:" + e.getMessage(), e);
        }
    }
    private static boolean dbExists(HCatClient client, final String database)
            throws Exception {
        try {
            HCatDatabase db = client.getDatabase(database);
            return db != null;
        } catch (HCatException e) {
            throw new Exception("Exception checking if the db exists:" + e.getMessage(), e);
        }
    }
}
