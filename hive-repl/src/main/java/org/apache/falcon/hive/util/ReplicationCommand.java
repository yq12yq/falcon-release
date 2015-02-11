/**
 * Interface that abstracts the notion of one atomic command to execute.
 * If the command does not execute and raises some exception, then Command
 * provides a conditional to check if the operation is intended to be
 * retriable - i.e. whether the command is considered idempotent. If it is,
 * then the user could attempt to redo the particular command they were
 * running. If not, then they can check another conditional to check
 * if their action is undo-able. If undoable, then they can then attempt
 * to undo the action by asking the command how to undo it. If not, they
 * can then in turn act upon the exception in whatever manner they see
 * fit (typically by raising an error).
 *
 * We also have two more methods that help cleanup of temporary locations
 * used by this Command. cleanupLocationsPerRetry() provides a list of
 * directories that are intended to be cleaned up every time this Command
 * needs to be retried. cleanupLocationsAfterEvent() provides a list of
 * directories that should be cleaned up after the event for which this
 * Command is generated is successfully processed.
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
