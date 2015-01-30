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

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.log4j.Logger;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

public class RecipeMerlin {
    private static final Logger LOGGER = Logger.getLogger(RecipeMerlin.class);
    private static final String WORKFLOW_PATH_KEY = "falcon.recipe.workflow.path";
    private static final String RECIPE_NAME_KEY = "falcon.recipe.name";
    private String template;
    private AbstractFileConfiguration properties;
    private String workflow;

    private RecipeMerlin() {
    }

    public String getName() {
        return properties.getString(RECIPE_NAME_KEY);
    }

    public void setName(String newName) {
        properties.setProperty(RECIPE_NAME_KEY, newName);
    }

    /**
     * Read recipe from a given directory. Expecting that recipe will follow these conventions.
     * <br> 1. properties file will have .properties extension
     * <br> 2. template file will have end with -template.xml
     * <br> 3. workflow file will have end with -workflow.xml
     * @param readPath the location from where recipe will be read
     */
    public static RecipeMerlin readFromDir(final String readPath) {
        RecipeMerlin instance = new RecipeMerlin();
        LOGGER.info("Loading recipe from directory: " + readPath);
        File directory = null;
        try {
            directory = new File(RecipeMerlin.class.getResource("/" + readPath).toURI());
        } catch (URISyntaxException e) {
            Assert.fail("could not find dir: " + readPath);
        }
        final Collection<File> propertiesFiles = FileUtils.listFiles(directory,
            new RegexFileFilter(".*\\.properties"), FalseFileFilter.INSTANCE);
        Assert.assertEquals(propertiesFiles.size(), 1,
            "Expecting only one property file at: " + readPath +" found: " + propertiesFiles);
        try {
            instance.properties =
                new PropertiesConfiguration(propertiesFiles.iterator().next());
        } catch (ConfigurationException e) {
            Assert.fail("Couldn't read recipe's properties file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
        instance.properties.setFileName(null); //prevent accidental overwrite of template

        final Collection<File> templatesFiles = FileUtils.listFiles(directory,
            new RegexFileFilter(".*-template\\.xml"), FalseFileFilter.INSTANCE);
        Assert.assertEquals(templatesFiles.size(), 1,
            "Expecting only one template file at: " + readPath + " found: " + templatesFiles);
        try {
            instance.template =
                FileUtils.readFileToString(templatesFiles.iterator().next());
        } catch (IOException e) {
            Assert.fail("Couldn't read recipe's template file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }

        final Collection<File> workflowFiles = FileUtils.listFiles(directory,
            new RegexFileFilter(".*-workflow\\.xml"), FalseFileFilter.INSTANCE);
        Assert.assertEquals(workflowFiles.size(), 1,
            "Expecting only one workflow file at: " + readPath + " found: " + workflowFiles);
        try {
            instance.workflow = FileUtils.readFileToString(workflowFiles.iterator().next());
        } catch (IOException e) {
            Assert.fail("Couldn't read recipe's workflow file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
        return instance;
    }

    /**
     * Write recipe to a given directory.
     * @param writeDir the location where recipe will be written
     */
    public void writeToDir(final String writeDir) {
        final String templateFileLocation = OSUtil.getPath(writeDir, getName() + "-template.xml");
        try {
            Assert.assertNotNull(templateFileLocation,
                "Write location for template file is unexpectedly null.");
            FileUtils.writeStringToFile(new File(templateFileLocation), template);
        } catch (IOException e) {
            Assert.fail("Couldn't write recipe's template file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }

        final String workflowFileLocation = OSUtil.getPath(writeDir, getName() + "-workflow.xml");
        try {
            Assert.assertNotNull(workflowFileLocation,
                "Write location for workflow file is unexpectedly null.");
            FileUtils.writeStringToFile(new File(workflowFileLocation), workflow);
        } catch (IOException e) {
            Assert.fail("Couldn't write recipe's workflow file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
        properties.setProperty(WORKFLOW_PATH_KEY, workflowFileLocation);

        final String propFileLocation = OSUtil.getPath(writeDir, getName() + ".properties");
        try {
            Assert.assertNotNull(propFileLocation,
                "Write location for properties file is unexpectedly null.");
            properties.save(new File(propFileLocation));
        } catch (ConfigurationException e) {
            Assert.fail("Couldn't write recipe's process file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
    }

}
