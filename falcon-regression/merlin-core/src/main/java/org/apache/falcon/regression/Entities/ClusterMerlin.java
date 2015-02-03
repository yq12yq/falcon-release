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

package org.apache.falcon.regression.Entities;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.ACL;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.regression.core.util.Util;
import org.apache.log4j.Logger;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Class for representing a cluster xml. */
public class ClusterMerlin extends Cluster {
    private static final Logger LOGGER = Logger.getLogger(ClusterMerlin.class);

    public ClusterMerlin(String clusterData) {
        final Cluster cluster = (Cluster) TestEntityUtil.fromString(EntityType.CLUSTER,
                clusterData);
        try {
            PropertyUtils.copyProperties(this, cluster);
        } catch (IllegalAccessException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (InvocationTargetException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (NoSuchMethodException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        }
    }

    public static List<ClusterMerlin> fromString(List<String> clusterStrings) {
        List<ClusterMerlin> clusters = new ArrayList<ClusterMerlin>();
        for (String clusterString : clusterStrings) {
            clusters.add(new ClusterMerlin(clusterString));
        }
        return clusters;
    }

    @Override
    public String toString() {
        try {
            StringWriter sw = new StringWriter();
            EntityType.CLUSTER.getMarshaller().marshal(this, sw);
            return sw.toString();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets unique names for the cluster.
     * @return mapping of old name to new name
     */
    public Map<? extends String, ? extends String> setUniqueName() {
        final String oldName = getName();
        final String newName =  oldName + Util.getUniqueString();
        setName(newName);
        final HashMap<String, String> nameMap = new HashMap<String, String>(1);
        nameMap.put(oldName, newName);
        return nameMap;
    }

    /**
     * Set ACL.
     */
    public void setACL(String owner, String group, String permission) {
        ACL acl = new ACL();
        acl.setOwner(owner);
        acl.setGroup(group);
        acl.setPermission(permission);
        this.setACL(acl);
    }

    public String getInterfaceEndpoint(final Interfacetype interfaceType) {
        String value = null;
        for (Interface anInterface : getInterfaces().getInterfaces()) {
            if (anInterface.getType() == interfaceType) {
                value = anInterface.getEndpoint();
            }
        }
        LOGGER.info("Cluster: " + getName() + " interfaceType: " + interfaceType
            + " value:" + value);
        return value;
    }

    public String getProperty(final String propName) {
        String value = null;
        for (Property property : getProperties().getProperties()) {
            if (property.getName().trim().equals(propName.trim())) {
                value = property.getValue();
            }
        }
        LOGGER.info("Cluster: " + getName() + " property: " + propName + " value:" + value);
        return value;
    }

    public String getLocation(final String locationType) {
        String value = null;
        for (Location location : getLocations().getLocations()) {
            if (location.getName().trim().equals(locationType.trim())) {
                value = location.getPath();
            }
        }
        LOGGER.info("Cluster: " + getName() + " locationType: " + locationType + " value:" + value);
        return value;
    }
}
