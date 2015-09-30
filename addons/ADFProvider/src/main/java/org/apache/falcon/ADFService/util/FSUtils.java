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

package org.apache.falcon.ADFService.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.ADFService.ADFJob;
import org.apache.falcon.FalconException;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utility for file operations.
 */
public final class FSUtils {

    public static String readTemplateFile(final String hdfsUrl, final String templateFilePath)
            throws IOException, URISyntaxException {
        FileSystem fs = FileSystem.get(new URI(hdfsUrl), new Configuration());
        Path pt = new Path(templateFilePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        StringBuilder fileContent = new StringBuilder();
        String line;
        while (true) {
            line = br.readLine();
            if (line == null) {
                break;
            }
            fileContent.append(line);
        }
        return fileContent.toString();
    }

    public static String createScriptFile(final String scriptContent,
                                          final String additionalProperties,
                                          final String fileName,
                                          final String fileExtension) throws FalconException {
        // path is unique as job name is always unique
        /* TODO - delete the file at the end */
        final Path path = new Path(ADFJob.PROCESS_SCRIPTS_PATH, fileName + fileExtension);
        OutputStream out = null;
        try {
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(path.toUri());
            HadoopClientFactory.mkdirsWithDefaultPerms(fs, path);
            out = fs.create(path);
            out.write(scriptContent.getBytes());
            if (StringUtils.isNotBlank(additionalProperties)) {
                out.write(additionalProperties.getBytes());
            }
        } catch (IOException e) {
            throw new FalconException("Error preparing script file: " + path, e);
        } finally {
            IOUtils.closeQuietly(out);
        }
        return path.toString();
    }

    private FSUtils() {
    }
}
