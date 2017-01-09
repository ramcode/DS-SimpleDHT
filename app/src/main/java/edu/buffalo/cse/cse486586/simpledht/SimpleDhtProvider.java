package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.DropBoxManager;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.simpledht.provider";
    static final String TABLE_NAME = SimpleDhtDB.TABLE_NAME;
    static final String KEY_COLUM = SimpleDhtDB.TABLE_COL_KEY;
    static final String URL = "content://" + PROVIDER_NAME;
    private static final Uri CONTENT_URI = Uri.parse(URL);
    private SimpleDhtDB simpleDhtDB;
    private static final UriMatcher uriMatcher =
            new UriMatcher(UriMatcher.NO_MATCH);
    static final int TABLE = 1;
    static final int ROW = 2;
    static final int SERVER_PORT = 10000;
    static String myPort = null;
    static Node myPredecessor = null;
    static Node mySuccessor = null;
    static String myAvdPort = null;
    static final String NODE_JOIN = "NODE_JOIN";
    static final String NODE_UPDATE = "NODE_UPDATE";
    static final String NODE_QUERY_MSG = "QUERY";
    static final String NODE_INSERT_MSG = "INSERT";
    static final String NODE_DELETE_MSG=  "DELETE";
    static final String NODE_JOIN_PORT = "11108";
    static final String NODE_QUERY_ALL = "QUERY_ALL";
    static final String NODE_QUERY_ALL_RESPONSE = "QUERY_ALL_RESPONSE";
    static final String NODE_DELETE_RESPONSE="DELETE_RESPONSE";
    static final String NODE_DELETE_ALL_RESPONSE = "DELETE_ALL_RESPONSE";
    static final String NODE_QUERY_RESPONSE="QUERY_RESPONSE";
    static final String NODE_DELETE_ALL = "DELETE_ALL";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String QUERY_ALL = "*";
    private static final String QUERY_ME = "@";
    boolean isResponseReceived = false;
    Map<String, String> queryDump = null;
    int totalRowsDeleted = 0;
    private static MatrixCursor matrixCursor = null;
    Object lock = null;
    static TreeMap<String, Node> chordMap = new TreeMap<String, Node>(new Comparator<String>() {
        @Override
        public int compare(String lhs, String rhs) {
            return lhs.compareTo(rhs);
        }
    });


    static {
        uriMatcher.addURI(PROVIDER_NAME, TABLE_NAME, TABLE);
        uriMatcher.addURI(PROVIDER_NAME, TABLE_NAME + "/#", ROW);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.v("query", selection);
        SQLiteDatabase sqlDB = simpleDhtDB.getWritableDatabase();
        int rowsDeleted = 0;
        try {
            if(selection.equals(QUERY_ME)) {
                Log.v(TAG, "Deleting all messages in avd: " + myAvdPort);
                rowsDeleted = sqlDB.delete(TABLE_NAME, "1", null);
            }
            else if(selection.equals(QUERY_ALL)){
                Message nodeMessage = new Message(selection);
                nodeMessage.setFromPort(myPort);
                nodeMessage.setOriginalRequester(getAvdPort(myPort));
                nodeMessage.setMessageType(NODE_DELETE_ALL);
                nodeMessage.setToPort(getNetworkPort(nodeMessage.getOriginalRequester()));
                new ClientThread(nodeMessage).start();
                synchronized (this.lock) {
                    try {
                        this.lock.wait();
                    } catch (InterruptedException ex) {
                        Log.d(TAG, "Response received: " + isResponseReceived);

                    }
                    rowsDeleted = totalRowsDeleted;
                    isResponseReceived = false;
                    totalRowsDeleted = 0;
                }
            }
            else {
                Log.d(TAG, "Deleting key: "+selection+" in avd: "+myAvdPort);
                if(lookUpKey(selection)){
                    Log.d(TAG, "Key: " + selection + " found in avd: " +myAvdPort);
                    rowsDeleted = sqlDB.delete(TABLE_NAME, KEY_COLUM + "=?", new String[]{selection});
                }
                else{
                    Log.d(TAG, "Routing delete query to avd: "+mySuccessor.getAvdPort());
                    Message nodeMessage = new Message(selection);
                    nodeMessage.setFromPort(myPort);
                    nodeMessage.setOriginalRequester(myAvdPort);
                    nodeMessage.setMessageType(NODE_DELETE_MSG);
                    nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
                    new ClientThread(nodeMessage).start();
                    synchronized (this.lock) {
                        try {
                            this.lock.wait();
                        } catch (InterruptedException ex) {
                            Log.d(TAG, "Response received: " + isResponseReceived);

                        }
                        rowsDeleted = totalRowsDeleted;
                        isResponseReceived = false;
                        totalRowsDeleted = 0;
                    }
                }
            }
            if (rowsDeleted > 0) {
                Log.d(TAG, "Delete Succeeded");
            } else {
                Log.d(TAG, "Delete failed for: " + selection);
            }
        } catch (Exception ex) {
            Log.d(TAG, "Delete failed for key: " + selection + " ,Exception: " + ex);
        }
        return rowsDeleted;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            SQLiteDatabase sqlDB = simpleDhtDB.getWritableDatabase();
            String key = (String) values.get(KEY_FIELD);
            String value = (String) values.get(VALUE_FIELD);
            Log.d(TAG, "Querying for key: "+key+" in avd: "+myAvdPort);
            if(lookUpKey(key)) {
                Log.d(TAG, "Key: " + key + " found in: " + myAvdPort);
                insertIntoDb(uri, values);
            }
            else{
                Log.d(TAG, "key: " + key + " not found in avd: " + myAvdPort);
                Log.d(TAG, "Routing to successor node: " + mySuccessor.getAvdPort());
                Message nodeMessage = new Message(key);
                nodeMessage.setFromPort(myPort);
                nodeMessage.setOriginalRequester(myAvdPort);
                nodeMessage.setMessageType(NODE_INSERT_MSG);
                nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
                Map<String, String> contentValues = new HashMap<String, String>();
                contentValues.put(KEY_FIELD, key);
                contentValues.put(VALUE_FIELD, value);
                nodeMessage.setContentValues(contentValues);
                new ClientThread(nodeMessage).start();
            }

        } catch (Exception ex) {
            Log.d(TAG, "Insert Row failed: " + values.toString() + " Exception: " + ex);
        }
        return uri;
    }

    private Uri insertIntoDb(Uri uri, ContentValues values) {
        try {
            SQLiteDatabase sqlDB = simpleDhtDB.getWritableDatabase();
            Log.d(TAG, "Inserting (key, value) " + values.toString() + "in avd: "+myAvdPort);
            long rowId = sqlDB.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
            if (rowId > 0) {
                Log.d(TAG, "Insert row success, (key, value) "+values.toString()+" RowId: " + rowId);
                getContext().getContentResolver().notifyChange(uri, null);
                return Uri.parse(TABLE_NAME + "/" + rowId);
            } else {
                throw new SQLException("Insert Row failed (key, value) : " + values.toString());
            }
        } catch (Exception ex) {
            Log.d(TAG, "Insert Row failed (key, value): " + values.toString() + " Exception: " + ex);
        }
        return uri;
    }

    private boolean lookUpKey(String key){
        boolean found = false;
        try {
            String hashedKey = genHash(key);
            String myNodeId = genHash(myAvdPort);
            if(myPredecessor.nodeId.equals(myNodeId) && mySuccessor.nodeId.equals(myNodeId)){ //I am the only node in the chord
                if(hashedKey.compareTo(myPredecessor.nodeId)>0 || hashedKey.compareTo(mySuccessor.nodeId)<=0){
                    found = true;
                }
            }
            else if(hashedKey.compareTo(myPredecessor.nodeId)>0 && hashedKey.compareTo(myNodeId)<=0){
                found = true;
            }
            else if(myPredecessor.nodeId.compareTo(myNodeId)>0 && (hashedKey.compareTo(myPredecessor.nodeId)>0 || hashedKey.compareTo(myNodeId)<=0)){
                found = true;
            }
            else {
                found = false;
            }
        }
        catch (NoSuchAlgorithmException ex){
            Log.d(TAG, "Hashing failed for key: "+key);
        }
        return found;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        // If you need to perform any one-time initialization task, please do it here.
        myPort = getMyPort();
        myAvdPort = getAvdPort(myPort);
        lock = new Object();
        Log.d(TAG, "Initializing DB in provider...");
        Context context = getContext();
        simpleDhtDB = new SimpleDhtDB(context);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, new Object[]{serverSocket, this});
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        Message nodeMessage = new Message(null);
        nodeMessage.setFromPort(myPort);
        nodeMessage.setMessageType(NODE_JOIN);
        nodeMessage.setToPort(NODE_JOIN_PORT);
        new ClientThread(nodeMessage).start();
        //waiting for system to stabilize
        synchronized (this.lock) {
            try {
                this.lock.wait();
            } catch (InterruptedException ex) {
                Log.d(TAG, "Response received: " + isResponseReceived);
            }
        }
        return simpleDhtDB.getWritableDatabase() == null ? false : true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        Log.v("query", selection);
        Cursor cursor = null;
        SQLiteDatabase sqlDB = simpleDhtDB.getWritableDatabase();
        try {
            if(selection.equals(QUERY_ME)) {
                Log.v(TAG, "Querying for all messages in avd: " + myAvdPort);
                cursor = sqlDB.query(TABLE_NAME, projection, null, null, null, null, null);
            }
            else if(selection.equals(QUERY_ALL)){
                Message nodeMessage = new Message(selection);
                nodeMessage.setFromPort(myPort);
                nodeMessage.setOriginalRequester(myAvdPort);
                nodeMessage.setMessageType(NODE_QUERY_ALL);
                nodeMessage.setToPort(getNetworkPort(nodeMessage.getOriginalRequester()));
                new ClientThread(nodeMessage).start();
                synchronized (this.lock) {
                    try {
                        Log.d(TAG, "I'm waiting......");
                        this.lock.wait();
                    } catch (InterruptedException ex) {
                        Log.d(TAG, "Response received: " + isResponseReceived);

                    }
                    Log.d(TAG, "Oops.....Response Received");
                    cursor = createCursor(queryDump);
                    isResponseReceived = false;
                    queryDump = null;
                }
            }
            else {
                Log.d(TAG, "Querying key: "+selection+" in avd: "+myAvdPort);
                if(lookUpKey(selection)){
                    Log.d(TAG, "Key: " + selection + " found in avd: " +myAvdPort);
                    cursor = sqlDB.query(TABLE_NAME, projection, KEY_COLUM+"=?", new String[]{selection}, null, null, null);
                }
                else{
                    Log.d(TAG, "Routing query to avd: " + mySuccessor.getAvdPort());
                    Message nodeMessage = new Message(selection);
                    nodeMessage.setFromPort(myPort);
                    nodeMessage.setOriginalRequester(myAvdPort);
                    nodeMessage.setMessageType(NODE_QUERY_MSG);
                    nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
                    new ClientThread(nodeMessage).start();
                    synchronized (this.lock) {
                        try {
                            this.lock.wait();
                        } catch (InterruptedException ex) {
                            Log.d(TAG, "Response received: " + isResponseReceived);

                        }
                        cursor = createCursor(queryDump);
                        isResponseReceived = false;
                        queryDump = null;
                    }
                }
            }
            if (cursor.getCount() > 0) {
                Log.d(TAG, "Thank God! Querying Succeeded");
                cursor.setNotificationUri(getContext().getContentResolver(), uri);
            } else {
                Log.d(TAG, "Querying failed for: " + selection);
            }
        } catch (Exception ex) {
            Log.d(TAG, "Query failed for key: " + selection + " ,Exception: " + ex);
        }
        return cursor;
    }

    private Cursor createCursor(Map<String, String> queryDump){
        MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
        Iterator<Map.Entry<String, String>> itr = queryDump.entrySet().iterator();
        while(itr.hasNext()){
            Map.Entry<String,String> entry = itr.next();
            cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
        }
        return cursor;
    }

    private Map<String, String> queryAll(){
        Log.v(TAG, "Querying for all messages in avd: " + myAvdPort);
        Map<String, String> myResults = new HashMap<String, String>();
        SQLiteDatabase sqlDB = simpleDhtDB.getWritableDatabase();
        Cursor cursor = sqlDB.query(TABLE_NAME, null, null, null, null, null, null);
        while(cursor.moveToNext()){
            int keyIndex = cursor.getColumnIndex(KEY_FIELD);
            int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
            String key = cursor.getString(keyIndex);
            String value = cursor.getString(valueIndex);
            myResults.put(key, value);
        }
        return myResults;
    }

    private Map<String, String> querySingle(String selection){
        Log.v(TAG, "Querying for key: "+selection +" in avd: " + myAvdPort);
        SQLiteDatabase sqlDB = simpleDhtDB.getWritableDatabase();
        Cursor cursor = sqlDB.query(TABLE_NAME, null, KEY_COLUM + "=?", new String[]{selection}, null, null, null);
        Map<String, String> myQueryResult = new HashMap<String, String>();
        while(cursor.moveToNext()){
            int keyIndex = cursor.getColumnIndex(KEY_FIELD);
            int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
            String key = cursor.getString(keyIndex);
            String value = cursor.getString(valueIndex);
            myQueryResult.put(key, value);
        }
        return myQueryResult;
    }

    private int deleteSingle(String selection){
        Log.v(TAG, "Querying for key: "+selection +" in avd: " + myAvdPort);
        SQLiteDatabase sqlDB = simpleDhtDB.getWritableDatabase();
        return sqlDB.delete(TABLE_NAME, KEY_COLUM+"=?", new String[]{selection});
    }

    private int deleteAll(String selection){
        Log.v(TAG, "Deleting all rows in avd: " + myAvdPort);
        SQLiteDatabase sqlDB = simpleDhtDB.getWritableDatabase();
        return sqlDB.delete(TABLE_NAME, "1", null);
    }

    private Map<String,String> addMyRowsToDump(Map<String,String> source, Map<String,String> destination){
        Iterator<Map.Entry<String, String>> itr = source.entrySet().iterator();
        while(itr.hasNext()){
            Map.Entry<String,String> entry = itr.next();
            destination.put(entry.getKey(), entry.getValue());
        }
        return destination;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    private class ClientThread extends Thread {
        public Message messageToSend;

        public ClientThread(Message messageToSend) {
            this.messageToSend = messageToSend;
        }

        @Override
        public void run() {
            Socket remoteSocket = null;
            String remotePort = messageToSend.getToPort();
            boolean isCoordinatorUp = true;
            try {
                remoteSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
            } catch (SocketTimeoutException ex) {
                Log.e(TAG, "Socket Timeout in  connecting to remote port: " + remotePort);
                if (messageToSend.getMessageType().equals(NODE_JOIN)) {
                    remotePort = myPort;
                    isCoordinatorUp = false;
                }
            } catch (UnknownHostException ex) {
                Log.e(TAG, "Remote port is not up yet: " + remotePort);
                if (messageToSend.getMessageType().equals(NODE_JOIN)) {
                    remotePort = myPort;
                    isCoordinatorUp = false;
                }
            } catch (IOException ex) {
                Log.e(TAG, "Exception in connecting to remote port: " + remotePort);
                if (messageToSend.getMessageType().equals(NODE_JOIN)) {
                    remotePort = myPort;
                    isCoordinatorUp = false;
                }
            } catch (Exception ex) {
                Log.e(TAG, "Exception in connecting to remote port: " + remotePort);
                if (messageToSend.getMessageType().equals(NODE_JOIN)) {
                    remotePort = myPort;
                    isCoordinatorUp = false;
                }
            }
            if (!isCoordinatorUp) {
                try {
                    remoteSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(myPort));
                } catch (Exception ex) {
                    Log.e(TAG, "Exception in connecting to remote port: " + myPort);
                }

            }
            if (remoteSocket != null && remoteSocket.isConnected()) {
                try {
                    OutputStream os = remoteSocket.getOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(os);
                    oos.writeObject(messageToSend);
                    oos.flush();
                    oos.close();
                } catch (Exception ex) {
                    Log.e(TAG, "Exception in sending message to: " + remotePort);
                    try {
                        remoteSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(myPort));
                        OutputStream os = remoteSocket.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(messageToSend);
                        oos.flush();
                        oos.close();
                    }
                    catch(Exception ex1){
                        Log.e(TAG, "Exception in connecting to socket: " + myPort);
                    }
                } finally {
                    try {
                        remoteSocket.close();
                    } catch (Exception ex) {
                        Log.e(TAG, "Exception in closing socket: " + remotePort);
                    }
                }
            }
        }
    }

    private class ServerTask extends AsyncTask<Object[], String, Void> {

        @Override
        protected Void doInBackground(Object[]... objects) {
            Object[] params = objects[0];
            ServerSocket serverSocket = (ServerSocket) params[0];
            SimpleDhtProvider dhtProvider = (SimpleDhtProvider) params[1];

            Socket socket = null;
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try {
                while (true) {
                    socket = serverSocket.accept();
                    InputStream in = socket.getInputStream();
                    ObjectInputStream ois = new ObjectInputStream(in);
                    Message nodeMessage = (Message) ois.readObject();
                    String messageType = nodeMessage.getMessageType();
                    String fromPort = nodeMessage.getFromPort();
                    String fromAvdPort = getAvdPort(fromPort);
                    switch (messageType){
                        case NODE_JOIN:
                            Log.d(TAG, "Node: " + fromAvdPort + " joined");
                            String nodeId = genHash(fromAvdPort);
                            chordMap.put(nodeId, new Node(fromAvdPort));
                            updateChord(chordMap);
                            //printChord(chordMap);
                            break;
                        case NODE_UPDATE:
                            Node node = nodeMessage.getNode();
                            if(node!=null){
                                myPredecessor = node.getPredecessor();
                                mySuccessor = node.getSuccessor();
                            }
                            synchronized (dhtProvider.lock) {
                                dhtProvider.lock.notify();
                            }
                            break;
                        case NODE_INSERT_MSG:
                            Map<String, String> rowToInsert = nodeMessage.getContentValues();
                            ContentValues contentValues = new ContentValues();
                            contentValues.put(KEY_FIELD, rowToInsert.get(KEY_FIELD));
                            contentValues.put(VALUE_FIELD, rowToInsert.get(VALUE_FIELD));
                            String key = (String) contentValues.get(KEY_FIELD);
                            if(lookUpKey(key)) {
                                Log.d(TAG, "Key: " + key + " found in: " + myPort);
                                insertIntoDb(CONTENT_URI, contentValues);
                            }
                            else{
                                Log.d(TAG, "key: "+key+" not found in avd: "+myPort);
                                Log.d(TAG, "Routing to successor node: "+mySuccessor.getAvdPort());
                                nodeMessage.setFromPort(myPort);
                                nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
                                new ClientThread(nodeMessage).start();
                            }
                            break;
                        case NODE_QUERY_ALL:
                            Map<String, String> myResults = queryAll();
                            Map<String, String> updatedDump = addMyRowsToDump(myResults, nodeMessage.getQueryDump()==null?new HashMap<String, String>(): nodeMessage.getQueryDump());
                            nodeMessage.setFromPort(myPort);
                            nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
                            nodeMessage.setQueryDump(updatedDump);
                            if(!mySuccessor.getAvdPort().equals(nodeMessage.originalRequester)){
                                new ClientThread(nodeMessage).start();
                            }
                            else{
                                nodeMessage.setMessageType(NODE_QUERY_ALL_RESPONSE);
                                new ClientThread(nodeMessage).start();
                            }
                            break;
                        case NODE_QUERY_ALL_RESPONSE:
                            synchronized (dhtProvider.lock) {
                                Log.d(TAG, "I'm notifying myself to stop waiting....");
                                dhtProvider.isResponseReceived = true;
                                dhtProvider.queryDump = nodeMessage.getQueryDump();
                                dhtProvider.lock.notify();
                            }
                            break;
                        case NODE_QUERY_MSG:
                            String queryKey = nodeMessage.getMessage();
                            nodeMessage.setFromPort(myPort);
                            if(lookUpKey(queryKey)) {
                                Log.d(TAG, "Key: " + queryKey + " found in: " + myAvdPort);
                                Map<String,String> queryResult = querySingle(queryKey);
                                nodeMessage.setQueryDump(queryResult);
                                nodeMessage.setMessageType(NODE_QUERY_RESPONSE);
                                nodeMessage.setToPort(getNetworkPort(nodeMessage.getOriginalRequester()));
                                new ClientThread(nodeMessage).start();
                            }
                            else{
                                Log.d(TAG, "key: "+queryKey+" not found in avd: "+myAvdPort);
                                Log.d(TAG, "Routing query to successor node: " + mySuccessor.getAvdPort());
                                nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
                                new ClientThread(nodeMessage).start();
                            }
                            break;
                        case NODE_QUERY_RESPONSE:
                            synchronized (dhtProvider.lock) {
                                Log.d(TAG, "Response Received....");
                                dhtProvider.isResponseReceived = true;
                                dhtProvider.queryDump = nodeMessage.getQueryDump();
                                dhtProvider.lock.notify();
                            }

                            break;
                        case NODE_DELETE_MSG:
                            String deleteKey = nodeMessage.getMessage();
                            nodeMessage.setFromPort(myPort);
                            if(lookUpKey(deleteKey)) {
                                Log.d(TAG, "Key: " + deleteKey + " found in: " + myAvdPort);
                                nodeMessage.setRowsDeleted(deleteSingle(deleteKey));
                                nodeMessage.setMessageType(NODE_DELETE_RESPONSE);
                                nodeMessage.setToPort(getNetworkPort(nodeMessage.getOriginalRequester()));
                                new ClientThread(nodeMessage).start();
                            }
                            else{
                                Log.d(TAG, "key: "+deleteKey+" not found in avd: "+myAvdPort);
                                Log.d(TAG, "Routing delete query to successor node: " + mySuccessor.getAvdPort());
                                nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
                                new ClientThread(nodeMessage).start();
                            }
                            break;
                        case NODE_DELETE_RESPONSE:
                            synchronized (dhtProvider.lock) {
                                dhtProvider.isResponseReceived = true;
                                dhtProvider.totalRowsDeleted = nodeMessage.getRowsDeleted();
                                dhtProvider.lock.notify();
                            }
                            break;
                        case NODE_DELETE_ALL:
                            int rowsDeletedTillNow = nodeMessage.getRowsDeleted()+deleteAll(nodeMessage.getMessage());
                            nodeMessage.setRowsDeleted(rowsDeletedTillNow);
                            nodeMessage.setFromPort(myPort);
                            nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
                            if(!mySuccessor.getAvdPort().equals(nodeMessage.originalRequester)){
                                new ClientThread(nodeMessage).start();
                            }
                            else{
                                nodeMessage.setMessageType(NODE_DELETE_ALL_RESPONSE);
                                new ClientThread(nodeMessage).start();
                            }
                            break;
                        case NODE_DELETE_ALL_RESPONSE:
                            synchronized (dhtProvider.lock) {
                                dhtProvider.isResponseReceived = true;
                                dhtProvider.totalRowsDeleted = nodeMessage.getRowsDeleted();
                                dhtProvider.lock.notify();
                            }
                            break;
                    }
                }
            } catch (Exception ex) {
                Log.e(TAG, "Exception in Receiving messages " + ex);

            }
            return null;
        }

    }

    private void printChord(TreeMap<String, Node> chordMap){
        Iterator<Map.Entry<String, Node>> itr = chordMap.entrySet().iterator();
        while (itr.hasNext()) {
            Node currentNode = itr.next().getValue();
            Log.d(TAG, "Node: "+currentNode.getAvdPort()+" Node Predecessor: "+currentNode.getPredecessor().getAvdPort()+" Node Successor: "+currentNode.getSuccessor().getAvdPort());
        }
    }

    private void updateSuccessorAndPredecessor(Node node, TreeMap<String, Node> chordMap){
            Iterator<Map.Entry<String, Node>> itr = chordMap.entrySet().iterator();
            Node predecessor = null;
            int track = 1;
            while(itr.hasNext()){
                Node currentNode = itr.next().getValue();
                if(node.getNodeId().equals(currentNode.nodeId)){
                    if(itr.hasNext()){
                        node.setSuccessor(itr.next().getValue());
                    }
                    else{
                        node.setSuccessor(chordMap.firstEntry().getValue());
                    }
                    if(track==1){
                        node.setPredecessor(chordMap.lastEntry().getValue());
                    }
                    else{
                        node.setPredecessor(predecessor);
                    }
                    break;
                }
                predecessor = currentNode;
                track++;
            }
    }

    private void updateChord(TreeMap<String, Node> chordMap) {
        Iterator<Map.Entry<String, Node>> itr = chordMap.entrySet().iterator();
        while (itr.hasNext()) {
            Node currentNode = itr.next().getValue();
            updateSuccessorAndPredecessor(currentNode, chordMap);
            Message message = new Message(null);
            message.setMessageType(NODE_UPDATE);
            message.setNode(currentNode);
            message.setFromPort(getMyPort());
            message.setToPort(String.valueOf(Integer.parseInt(currentNode.getAvdPort()) * 2));
            message.setMessageType(NODE_UPDATE);
            new ClientThread(message).start();
        }
    }

    private String getMyPort() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return myPort;
    }

    private String getAvdPort(String port){
        return String.valueOf((Integer.parseInt(port)/2));
    }

    private String getNetworkPort(String avdPort){
        return String.valueOf((Integer.parseInt(avdPort)*2));
    }
}
