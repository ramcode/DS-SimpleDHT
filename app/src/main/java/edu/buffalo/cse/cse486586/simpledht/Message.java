package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.database.MatrixCursor;

import java.io.Serializable;
import java.security.MessageDigestSpi;
import java.util.Map;

/**
 * Created by ramesh on 2/19/16.
 */
public class Message implements Serializable {

    public String message;
    public String avdPort;
    public String originalRequester;
    public Map<String, String> contentValues;
    public Map<String, String> queryDump;

    public Map<String, String> getQueryDump() {
        return queryDump;
    }

    public void setQueryDump(Map<String, String> queryDump) {
        this.queryDump = queryDump;
    }

    public Map<String, String> getContentValues() {
        return contentValues;
    }

    public void setContentValues(Map<String, String> contentValues) {
        this.contentValues = contentValues;
    }

    public int getRowsDeleted() {
        return rowsDeleted;
    }

    public void setRowsDeleted(int rowsDeleted) {
        this.rowsDeleted = rowsDeleted;
    }

    public int rowsDeleted;

    public String getOriginalRequester() {
        return originalRequester;
    }

    public void setOriginalRequester(String originalRequester) {
        this.originalRequester = originalRequester;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Node node;

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String fromPort;
    public String messageType;

    public String getToPort() {
        return toPort;
    }

    public void setToPort(String toPort) {
        this.toPort = toPort;
    }

    public String toPort;


    public Message(String message){
        this.message = message;
    }
    public Message(String message, String avd){
        this.message = message;
        this.avdPort = avd;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getAvdPort() {
        return avdPort;
    }

    public void setAvdPort(String avdPort) {
        this.avdPort = avdPort;
    }

    public String getFromPort() {
        return fromPort;
    }

    public void setFromPort(String fromPort) {
        this.fromPort = fromPort;
    }
}
