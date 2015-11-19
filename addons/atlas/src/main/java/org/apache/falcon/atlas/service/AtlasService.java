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

package org.apache.falcon.atlas.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.atlas.Util.EventUtil;
import org.apache.falcon.atlas.event.FalconClusterEvent;
import org.apache.falcon.atlas.event.FalconDatasourceEvent;
import org.apache.falcon.atlas.event.FalconEvent;
import org.apache.falcon.atlas.event.FalconFeedEvent;
import org.apache.falcon.atlas.event.FalconProcessEvent;
import org.apache.falcon.atlas.publisher.FalconEventPublisher;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.metadata.util.MetadataUtil;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowExecutionListener;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Atlas service to publish Falcon events
 */
public class AtlasService implements FalconService, ConfigurationChangeListener, WorkflowExecutionListener {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasService.class);
    private static final String PUBLISHER_CLASS_NAME = "falcon.event.data.publish.class";
    private FalconEventPublisher publisher;

    /**
     * Constant for the service name.
     */
    public static final String SERVICE_NAME = AtlasService.class.getSimpleName();

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        ConfigurationStore.get().registerListener(this);
        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).registerListener(this);

        publisher = getPublisher();
    }


    @Override
    public void destroy() throws FalconException {
        ConfigurationStore.get().unregisterListener(this);
        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).unregisterListener(this);
    }

    @Override
    public void onAdd(Entity entity) throws FalconException {
        EntityType entityType = entity.getEntityType();
        switch (entityType) {
            case CLUSTER:
                addClusterEntity((Cluster) entity);
                break;
            case PROCESS:
            case FEED:
                LOG.info("Feed or process entities not added to Atlas");
                break;
            case DATASOURCE:
                addDatasourceEntity((Datasource) entity);
                break;

            default:
                LOG.error("Invalid EntityType " + entityType);
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        /* TODO : Atlas doesnt have the implementation for delete */
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        /* TODO : Update not implemented currently */
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        /* No change required, return */
    }

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException {
        LOG.info("Adding Atlas lineage for context {}", context);
        WorkflowExecutionContext.EntityOperations entityOperation = context.getOperation();
        switch (entityOperation) {
            case GENERATE:
                onProcessInstanceExecuted(context);
                break;
            case REPLICATE:
                onFeedInstanceReplicated(context);
                break;
            case DELETE:
                onFeedInstanceEvicted(context);
                break;
            case IMPORT:
                onFeedInstanceImported(context);
                break;
            default:
                throw new IllegalArgumentException("Invalid EntityOperation - " + entityOperation);
        }
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {
        // do nothing since lineage is only recorded for successful workflow
    }

    private FalconEventPublisher getPublisher() throws FalconException {
        String publishClassName = StartupProperties.get().getProperty(PUBLISHER_CLASS_NAME);
        if (StringUtils.isBlank(publishClassName)) {
            throw new FalconException(PUBLISHER_CLASS_NAME + " not set in startup properties, please add it.");
        }

        FalconEventPublisher publisher;
        try {
            Class publishClass = Class.forName(publishClassName);

            Object o = publishClass.newInstance();
            if (o instanceof FalconEventPublisher) {
                publisher = (FalconEventPublisher) o;
            } else {
                throw new FalconException("Object not instance of Falcon publisher");
            }
        } catch (ClassNotFoundException ex) {
            throw new FalconException("Not able to find the publisher class: " + ex.getMessage(), ex);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new FalconException("Not able to instantiate the publisher class: " + e.getMessage(), e);
        }
        return publisher;
    }

    private void publishDataToAtlas(FalconEvent event) throws FalconException {
        try {
            FalconEventPublisher.Data data = new FalconEventPublisher.Data(event);
            publisher.publish(data);
        } catch (Exception ex) {
            throw new FalconException("Unable to publish data to publisher " + ex.getMessage(), ex);
        }
    }

    private void addClusterEntity(Cluster entity) throws FalconException {
        LOG.info("Adding cluster entity to Atlas: {}", entity.getName());

        FalconClusterEvent clusterEvent = new FalconClusterEvent(CurrentUser.getUser(), EventUtil.getUgi(),
                FalconEvent.OPERATION.ADD_CLUSTER_ENTITY,
                entity.getName(), "CLUSTER", System.currentTimeMillis(),
                EventUtil.convertKeyValueStringToMap(entity.getTags()),
                entity.getColo());

        publishDataToAtlas(clusterEvent);
    }

    private void addDatasourceEntity(Datasource entity) throws FalconException {
        LOG.info("Adding data source entity to Atlas: {}", entity.getName());


        FalconDatasourceEvent datasourceEvent = new FalconDatasourceEvent(CurrentUser.getUser(),
                EventUtil.getUgi(),
                FalconEvent.OPERATION.ADD_DATASOURCE_ENTITY,
                entity.getName(), "DATASOURCE", System.currentTimeMillis(),
                EventUtil.convertKeyValueStringToMap(entity.getTags()),
                entity.getColo());

        publishDataToAtlas(datasourceEvent);
    }

    private void onProcessInstanceExecuted(WorkflowExecutionContext context) throws FalconException {
        String processInstanceName = MetadataUtil.getProcessInstanceName(context);
        LOG.info("Adding process instance to Atlas: {}", processInstanceName);

        org.apache.falcon.entity.v0.process.Process process =
                ConfigurationStore.get().get(EntityType.PROCESS, context.getEntityName());

        // TODO : verify counter is speartaed by = and ,
        FalconProcessEvent processEvent = new FalconProcessEvent(CurrentUser.getUser(), EventUtil.getUgi(),
                FalconEvent.OPERATION.ADD_PROCESS_INSTANCE,
                processInstanceName, "PROCESS", context.getTimeStampAsLong(),
                EventUtil.convertKeyValueStringToMap(process.getTags()),
                getInputFeedInstances(context), getOutputFeedInstances(context),
                context.getClusterName(),
                EventUtil.convertStringToList(process.getPipelines()),
                EventUtil.getWFProperties(context),
                EventUtil.convertKeyValueStringToMap(context.getCounters()));

        publishDataToAtlas(processEvent);
    }

    private void onFeedInstanceReplicated(final WorkflowExecutionContext context)
            throws FalconException {
        // For replication there will be only one output feed name and path
        String feedName = context.getOutputFeedNames();
        String feedInstanceDataPath = context.getOutputFeedInstancePaths();
        String targetClusterName = context.getClusterName();

        String feedInstanceName = MetadataUtil.getFeedInstanceName(feedName, targetClusterName,
                feedInstanceDataPath, context.getNominalTimeAsISO8601());

        Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
        List<String> paths = new ArrayList<>();
        paths.add(feedInstanceDataPath);
        FalconFeedEvent feedEvent = constructFeedEvent(feedInstanceName,
                FalconEvent.OPERATION.ADD_REPLICATED_FEED_INSTANCE,
                context, "REPLICATEDFEED", feed, null, paths);

        publishDataToAtlas(feedEvent);
    }

    private void onFeedInstanceEvicted(final WorkflowExecutionContext context)
            throws FalconException {
        final String outputFeedPaths = context.getOutputFeedInstancePaths();
        if (!MetadataUtil.hasFeeds(outputFeedPaths)) {
            LOG.info("There were no evicted instances, nothing to record");
            return;
        }

        String feedName = context.getOutputFeedNames();
        String[] feedNames = {feedName};

        String[] evictedFeedInstancePathList = context.getOutputFeedInstancePathsList();
        List<FalconFeedEvent> events = getFeedInstances(feedNames, evictedFeedInstancePathList, context,
                "EVICTEDFEED", null, FalconEvent.OPERATION.ADD_EVICTED_FEED_INSTANCE,
                Arrays.asList(evictedFeedInstancePathList));

        // TODO: Verify if batch?
        for (FalconFeedEvent event : events) {
            publishDataToAtlas(event);
        }
    }

    private void onFeedInstanceImported(WorkflowExecutionContext context) throws FalconException {
        String feedName = context.getOutputFeedNames();
        String feedInstanceDataPath = context.getOutputFeedInstancePaths();
        String sourceClusterName = context.getSrcClusterName();


        String feedInstanceName = MetadataUtil.getFeedInstanceName(feedName, sourceClusterName,
                feedInstanceDataPath, context.getNominalTimeAsISO8601());

        Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
        List<String> paths = new ArrayList<>();
        paths.add(feedInstanceDataPath);
        FalconFeedEvent feedEvent = constructFeedEvent(feedInstanceName,
                FalconEvent.OPERATION.ADD_GENERATED_FEED_INSTANCE,
                context, "IMPORTEDFEED", feed, context.getDatasourceName(), paths);

        publishDataToAtlas(feedEvent);
    }

    private static List<FalconFeedEvent> getOutputFeedInstances(WorkflowExecutionContext context)
            throws FalconException {
        String outputFeedNamesArg = context.getOutputFeedNames();
        if (!MetadataUtil.hasFeeds(outputFeedNamesArg)) {
            return null;
        }

        String[] outputFeedNames = context.getOutputFeedNamesList();
        String[] outputFeedInstancePaths = context.getOutputFeedInstancePathsList();

        /* TODO : verify paths in feed */
        return getFeedInstances(outputFeedNames, outputFeedInstancePaths, context,
                "OUTPUTFEED", null, FalconEvent.OPERATION.ADD_GENERATED_FEED_INSTANCE,
                Arrays.asList(outputFeedInstancePaths));
    }

    private static List<FalconFeedEvent> getInputFeedInstances(WorkflowExecutionContext context)
            throws FalconException {
        String inputFeedNamesArg = context.getInputFeedNames();
        if (!MetadataUtil.hasFeeds(inputFeedNamesArg)) {
            return null;
        }

        String[] inputFeedNames = context.getInputFeedNamesList();
        String[] inputFeedInstancePaths = context.getInputFeedInstancePathsList();

        /* TODO : verify paths in feed */
        return getFeedInstances(inputFeedNames, inputFeedInstancePaths, context,
                "INPUTFEED", null, FalconEvent.OPERATION.ADD_GENERATED_FEED_INSTANCE,
                Arrays.asList(inputFeedInstancePaths));
    }

    private static List<FalconFeedEvent> getFeedInstances(final String[] feedNames,
                                                          final String[] feedInstancePaths,
                                                          final WorkflowExecutionContext context,
                                                          final String feedType,
                                                          final String datasource,
                                                          final FalconEvent.OPERATION operation,
                                                          final List<String> paths
                                                          ) throws FalconException {
        List<FalconFeedEvent> feedEvents = new ArrayList<>();
        for (int index = 0; index < feedInstancePaths.length; ++index) {
            String feedName;
            if (feedNames.length == 1) {
                feedName = feedNames[0];
            } else {
                feedName = feedNames[index];
            }

            String feedInstanceDataPath = feedInstancePaths[index];
            FalconFeedEvent event =
                    getFeedInstance(context, feedName, feedInstanceDataPath, feedType, datasource,
                            operation, paths);
            feedEvents.add(event);
        }
        return feedEvents;
    }

    private static FalconFeedEvent getFeedInstance(WorkflowExecutionContext context, String feedName,
                                        String feedInstanceDataPath,
                                        final String feedType,
                                        final String datasource,
                                        final FalconEvent.OPERATION operation,
                                        final List<String> paths) throws FalconException {
        String clusterName = context.getClusterName();
        LOG.info("Computing feed instance for : name= {} path= {}, in cluster: {} for Atlas", feedName,
                feedInstanceDataPath, clusterName);
        String feedInstanceName = MetadataUtil.getFeedInstanceName(feedName, clusterName,
                feedInstanceDataPath, context.getNominalTimeAsISO8601());
        Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
        return constructFeedEvent(feedInstanceName, operation,
                context, feedType, feed, datasource, paths);
    }

    private static FalconFeedEvent constructFeedEvent(final String feedInstanceName,
                                                      final FalconEvent.OPERATION operation,
                                                      final WorkflowExecutionContext context,
                                                      final String feedType,
                                                      final Feed feedEntity,
                                                      final String dataSource,
                                                      final List<String> paths
    ) throws FalconException {
        LOG.info("Constructing feed entity for Atlas: {}", feedInstanceName);

        return new FalconFeedEvent(CurrentUser.getUser(), EventUtil.getUgi(),
                operation,
                feedInstanceName, feedType, context.getTimeStampAsLong(),
                EventUtil.convertKeyValueStringToMap(feedEntity.getTags()),
                dataSource,
                context.getSrcClusterName(),
                context.getClusterName(),
                EventUtil.convertStringToList(feedEntity.getGroups()),
                paths,
                EventUtil.convertKeyValueStringToMap(context.getCounters()));
    }
}
