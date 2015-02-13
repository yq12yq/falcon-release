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

package org.apache.falcon.hive.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public final class FileUtils {

    public static Configuration getConfiguration(final String writeEP) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", writeEP);
        return conf;
    }

    public static void validatePath(final FileSystem fileSystem, final Path basePath) throws IOException {
        if (fileSystem.exists(basePath)) {
            if (!fileSystem.getFileStatus(basePath).getPermission().equals(DRStatusStore.DEFAULT_STORE_PERMISSION)) {
                throw new IOException("Base dir " + fileSystem.getUri() + DRStatusStore.BASE_DEFAULT_STORE_PATH
                        + "does not have correct permissions. Please set to 777");
            }
        } else {
            throw new IOException("Please create base dir " + fileSystem.getUri() + DRStatusStore.BASE_DEFAULT_STORE_PATH +
                    " with permission 777.");
        }
    }
}
