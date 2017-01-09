package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/**
 * Created by ramesh on 4/4/16.
 */
public class Node implements Serializable {

    static final String TAG = Node.class.getSimpleName();

    public String nodeId;
    public Node successor;
    public Node predecessor;
    public String avdPort;

    public Node(String avdPort){
        this.avdPort = avdPort;
        try {
            this.nodeId = genHash(avdPort);
        }
        catch(NoSuchAlgorithmException ex){
            Log.d(TAG, "Exception in generating hash for: "+avdPort);
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public Node getSuccessor() {
        return successor;
    }

    public void setSuccessor(Node successor) {
        this.successor = successor;
    }

    public Node getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(Node predecessor) {
        this.predecessor = predecessor;
    }

    public String getAvdPort() {
        return avdPort;
    }

    public void setAvdPort(String avdPort) {
        this.avdPort = avdPort;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
