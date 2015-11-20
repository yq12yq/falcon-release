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

package org.apache.falcon.metadata.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.Path;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.TimeZone;

/**
 * Falcon metadata util.
 */
public final class MetadataUtil {
    private static final String NONE = "NONE";
    private static final String IGNORE = "IGNORE";

    private MetadataUtil() {}

    public static String getProcessInstanceName(WorkflowExecutionContext context) {
        return context.getEntityName() + "/" + context.getNominalTimeAsISO8601();
    }

    public static String getFeedInstanceName(String feedName, String clusterName,
                                             String feedInstancePath,
                                             String nominalTime) throws FalconException {
        try {
            Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
            Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER, clusterName);

            Storage.TYPE storageType = FeedHelper.getStorageType(feed, cluster);
            return storageType == Storage.TYPE.TABLE
                    ? getTableFeedInstanceName(feed, feedInstancePath, storageType)
                    : getFileSystemFeedInstanceName(feedInstancePath, feed, cluster, nominalTime);

        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }
    }

    public static boolean hasFeeds(final String feedNames) {
        return !(NONE.equals(feedNames) || IGNORE.equals(feedNames));
    }


    private static String getTableFeedInstanceName(Feed feed, String feedInstancePath,
                                                   Storage.TYPE storageType) throws URISyntaxException {
        CatalogStorage instanceStorage = (CatalogStorage) FeedHelper.createStorage(
                storageType.name(), feedInstancePath);
        return feed.getName() + "/" + instanceStorage.toPartitionAsPath();
    }

    private static String getFileSystemFeedInstanceName(String feedInstancePath, Feed feed,
                                                        Cluster cluster,
                                                        String nominalTime) throws FalconException {
        Storage rawStorage = FeedHelper.createStorage(cluster, feed);
        String feedPathTemplate = rawStorage.getUriTemplate(LocationType.DATA);
        String instance = feedInstancePath;

        String[] elements = FeedDataPath.PATTERN.split(feedPathTemplate);
        for (String element : elements) {
            instance = instance.replaceFirst(element, "");
        }

        Date instanceTime = FeedHelper.getDate(feedPathTemplate,
                new Path(feedInstancePath), TimeZone.getTimeZone("UTC"));

        return StringUtils.isEmpty(instance)
                ? feed.getName() + "/" + nominalTime
                : feed.getName() + "/"
                + SchemaHelper.formatDateUTC(instanceTime);
    }

    public static String getImportDatasourceName(Feed feed) {
        String feedDatasourceName = null;
        for (org.apache.falcon.entity.v0.feed.Cluster feedCluster : feed.getClusters().getClusters()) {
            if (FeedHelper.isImportEnabled(feedCluster)) {
                feedDatasourceName = FeedHelper.getImportDatasourceName(feedCluster);
                if (feedDatasourceName != null) {
                    break;
                }
            }
        }
        return feedDatasourceName;
    }
}
