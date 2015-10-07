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

package org.apache.falcon.ADFService;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.ADFService.util.FSUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionListener;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMode;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Falcon ADF provider to handle requests from Azure Data Factory.
 */
public class ADFProviderService implements FalconService, WorkflowExecutionListener {

    private static final Logger LOG = LoggerFactory.getLogger(ADFProviderService.class);

    /**
     * Constant for the service name.
     */
    public static final String SERVICE_NAME = ADFProviderService.class.getSimpleName();

    private ServiceBusContract service;

    private ScheduledExecutorService adfScheduledExecutorService;

    private ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;

    private static final int AZURE_SERVICEBUS_RECEIVEMESSGAEOPT_TIMEOUT = 60;
    // polling frequency in seconds
    private static final int AZURE_SERVICEBUS_DEFAULT_POLLING_FREQUENCY = 60;

    // Number of threads to handle ADF requests
    private static final int AZURE_SERVICEBUS_REQUEST_HANDLING_THREADS = 5;

    private static final String AZURE_SERVICEBUS_CONF_SASKEYNAME = "sasKeyName";
    private static final String AZURE_SERVICEBUS_CONF_SASKEY = "sasKey";
    private static final String AZURE_SERVICEBUS_CONF_SERVICEBUSROOTURI = "serviceBusRootUri";
    private static final String  AZURE_SERVICEBUS_CONF_NAMESPACE = "namespace";
    private static final String  AZURE_SERVICEBUS_CONF_POLLING_FREQUENCY = "polling.frequency";
    private static final String AZURE_SERVICEBUS_CONF_PREFIX = "microsoft.windowsazure.services.servicebus.";

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        service = ServiceBusService.create(getServiceBusConfig());

        // init opts
        opts.setReceiveMode(ReceiveMode.PEEK_LOCK);
        opts.setTimeout(AZURE_SERVICEBUS_RECEIVEMESSGAEOPT_TIMEOUT);

        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).registerListener(this);

        adfScheduledExecutorService = new ADFScheduledExecutor(AZURE_SERVICEBUS_REQUEST_HANDLING_THREADS);
        adfScheduledExecutorService.scheduleWithFixedDelay(new HandleADFRequests(), 0, getDelay(), TimeUnit.SECONDS);

        LOG.info("Falcon ADFProvider service initialized");

        try {
            String template = FSUtils.readTemplateFile(ADFJob.HDFS_URL_PORT,
                    ADFJob.TEMPLATE_PATH_PREFIX + ADFReplicationJob.TEMPLATE_REPLIACATION_FEED);
            LOG.info("replication template: " + template);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class HandleADFRequests implements Runnable {

        @Override
        public void run() {

            /* TODO- Handle request */



        }
    }

    private static Configuration getServiceBusConfig() throws FalconException {
        String namespace = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_NAMESPACE);
        if (namespace == null) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_NAMESPACE
                    + " property not set in startup properties. Please add it.");
        }

        String sasKeyName = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_SASKEYNAME);
        if (sasKeyName == null) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_SASKEYNAME
                    + " property not set in startup properties. Please add it.");
        }
        String sasKey = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_SASKEY);
        if (sasKey == null) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_SASKEY
                    + " property not set in startup properties. Please add it.");
        }

        String serviceBusRootUri = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_SERVICEBUSROOTURI);
        if (serviceBusRootUri == null) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_SERVICEBUSROOTURI
                    + " property not set in startup properties. Please add it.");
        }

        return ServiceBusConfiguration.configureWithSASAuthentication(namespace, sasKeyName, sasKey,
                serviceBusRootUri);
    }


    // gets delay in seconds
    private long getDelay() throws FalconException {
        String pollingFrequencyValue = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_POLLING_FREQUENCY);
        long pollingFrequency;
        try {
            pollingFrequency = (StringUtils.isNotEmpty(pollingFrequencyValue))
                    ? Long.valueOf(pollingFrequencyValue) : AZURE_SERVICEBUS_DEFAULT_POLLING_FREQUENCY;
        } catch (NumberFormatException nfe) {
            throw new FalconException("Invalid value provided for startup property "
                    + AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_POLLING_FREQUENCY
                    + ", please provide a valid long number", nfe);
        }
        return pollingFrequency;
    }

    @Override
    public void destroy() throws FalconException {
        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).unregisterListener(this);
        adfScheduledExecutorService.shutdown();
    }

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException {

    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {

    }
}
