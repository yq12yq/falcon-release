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

import org.apache.commons.lang3.StringEscapeUtils;

public class DelimiterUtils {

    public static final String FIELD_DELIM = ",";
    public static final String STMT_DELIM = "|";
    public static final String RECORD_DELIM = "=====";

    public static String getEscapedFieldDelim() {
        return StringEscapeUtils.escapeJava(FIELD_DELIM);
    }

    public static String getEscapedStmtDelim() {
        return StringEscapeUtils.escapeJava(STMT_DELIM);
    }

    public static String getEscapedRecordDelim() {
        return StringEscapeUtils.escapeJava(RECORD_DELIM);
    }
}
