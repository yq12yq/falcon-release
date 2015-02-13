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

import org.apache.commons.lang.StringUtils;
import org.apache.hive.hcatalog.api.repl.Command;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ReplicationCommand implements Command {
    private List<String> events;
    private boolean isRetriable;
    private boolean isUndoable;
    private List<String> undo;
    private List<String> cleanupLocationsPerRetry;
    private List<String> cleanupLocationsAfterEvent;
    private long eventId;

    public ReplicationCommand(List<String> events, boolean isRetriable, boolean isUndoable, List<String> undo,
                              List<String> cleanupLocationsPerRetry, List<String> cleanupLocationsAfterEvent,
                              long eventId) {
        this.events = events;
        this.isRetriable = isRetriable;
        this.isUndoable = isUndoable;
        this.undo = undo;
        this.cleanupLocationsPerRetry = cleanupLocationsPerRetry;
        this.cleanupLocationsAfterEvent = cleanupLocationsAfterEvent;
        this.eventId = eventId;
    }

    public static ReplicationCommand parseCommandString(String commandString) {
        String[] str = commandString.split(DelimiterUtils.getCommandFieldDelim(),-1);
        return new ReplicationCommand(Arrays.asList(str[0].split(DelimiterUtils.getCommandEventDelim())),
                Boolean.parseBoolean(str[1]),
                Boolean.parseBoolean(str[2]),
                Arrays.asList(StringUtils.splitByWholeSeparator(str[3], DelimiterUtils.getCommandEventDelim())),
                Arrays.asList(StringUtils.splitByWholeSeparator(str[4], DelimiterUtils.getCommandEventDelim())),
                Arrays.asList(StringUtils.splitByWholeSeparator(str[5], DelimiterUtils.getCommandEventDelim())),
                Long.parseLong(str[6].trim()));
    }

    public List<String> get() {
        return events;
    }

    public boolean isRetriable() {
        return isRetriable;
    }

    public boolean isUndoable() {
        return isUndoable;
    }

    public List<String> getUndo() {
        return undo;
    }

    public List<String> cleanupLocationsPerRetry() {
        return cleanupLocationsPerRetry;
    }

    public List<String> cleanupLocationsAfterEvent() {
        return cleanupLocationsAfterEvent;
    }

    public long getEventId() {
        return eventId;
    }

    public void write(DataOutput out) throws IOException {
        //TODO: Nothing to do
    }

    public void readFields(DataInput in) throws IOException {
        //TODO: Nothing to do
    }

    public String toString() {
        return StringUtils.join(events.toArray(), DelimiterUtils.getCommandEventDelim())
                + DelimiterUtils.getCommandFieldDelim()
                + isRetriable
                + DelimiterUtils.getCommandFieldDelim()
                + isUndoable
                + DelimiterUtils.getCommandFieldDelim()
                + ((isUndoable) ? StringUtils.join(undo.toArray(), DelimiterUtils.getCommandEventDelim()) : "")
                + DelimiterUtils.getCommandFieldDelim()
                + StringUtils.join(cleanupLocationsPerRetry.toArray(), DelimiterUtils.getCommandEventDelim())
                + DelimiterUtils.getCommandFieldDelim()
                + StringUtils.join(cleanupLocationsAfterEvent.toArray(), DelimiterUtils.getCommandEventDelim())
                + DelimiterUtils.getCommandFieldDelim()
                + String.valueOf(eventId);
    }
}
