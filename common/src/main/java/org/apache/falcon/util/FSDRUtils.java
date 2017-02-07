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

package org.apache.falcon.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public final class FSDRUtils {
    private FSDRUtils() {
    }

    public static boolean isHCFS(Path filePath) throws FalconException {
        if (filePath == null) {
            throw new FalconException("filePath cannot be empty");
        }

        String scheme;
        try {
            FileSystem f = FileSystem.get(filePath.toUri(), new Configuration());
            scheme = f.getScheme();
            if (StringUtils.isBlank(scheme)) {
                throw new FalconException("Cannot get valid scheme for " + filePath);
            }
        } catch (IOException e) {
            throw new FalconException(e);
        }

        return (scheme.toLowerCase().contains("hdfs") ? false : true);
    }
}
