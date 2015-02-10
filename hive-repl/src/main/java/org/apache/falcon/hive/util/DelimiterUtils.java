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

public class DelimiterUtils {

    public static final String FIELD_DELIM = ",";
    public static final String RECORD_FIELD_DELIM = "\t";
    public static final String STMT_DELIM = ";";
    public static final String NEWLINE_DELIM = "\n";
    public static final String COMMAND_FIELD_DELIM = ":";

    public static String getEventStmtDelim() {
        return STMT_DELIM;
    }

    public static String getRecordNewLineDelim() {
        return NEWLINE_DELIM;
    }

    public static String getCommandEventDelim() { return FIELD_DELIM; }

    public static String getCommandFieldDelim() { return COMMAND_FIELD_DELIM; }

    public static String getRecordFieldDelim() { return RECORD_FIELD_DELIM; }
}