package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;

public class SimpleDhtProvider extends ContentProvider {


    static final Uri M_URI = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    // Types of requests
    static final String JOIN = "JOIN";
    static final String[] CLIENT_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String SUCCESS = "1";
    static final String INSERT_SELECTION = "INSERT_SELECTION";
    static final String DELETE_ALL = "DELETE_ALL";
    static final String DELETE_YOUR_VALUES = "DELETE_YOUR_VALUES";
    static final String DELETE_SELECTION = "DELETE_SELECTION";
    static final String QUERY_ALL = "QUERY_ALL";
    static final String QUERY_YOUR_VALUES = "QUERY_YOUR_VALUES";
    static final String QUERY_SELECTION = "QUERY_SELECTION";
    static final String FIND_AVD = "FIND_AVD";
    static final String JOIN_PORT = "11108";
    static final char SPLITTER = '|';
    static final int SERVER_PORT = 10000;
    static final int TIMEOUT_DELAY = 2000;

    static String predecessorID, successorID;
    static String localPort, predecessorPort, successorPort;

    // Only used by 5554
    ArrayList<Node> nodeList = new ArrayList<>();


    class Message {
        String msg, priority, senderPort;

        Message(String proposedPriority, String senderPort, String msg) {
            this.priority = proposedPriority;
            this.senderPort = senderPort;
            this.msg = msg;
        }
    }

    class Node {
        String avdNumber, node, predecessorID, successorID, predecessorPort, successorPort;

        Node(String avdNumber, String node, String predecessor, String successor, String pPort, String sPort) {
            this.avdNumber = avdNumber;
            this.node = node;
            this.predecessorID = predecessor;
            this.successorID = successor;
            this.predecessorPort = pPort;
            this.successorPort = sPort;
        }
    }

    // Split line into 3 sections based on |
    // port|requestType|message
    private String[] splitInput(String line) {
        String[] arr = new String[4];
        int count = 0;
        arr[count] = "";

        for (int i = 0; i < line.length(); i++) {
            if (count == arr.length - 1) {
                arr[count] += line.charAt(i);
            } else if (line.charAt(i) == SPLITTER) {
                count++;
                arr[count] = "";
            } else {
                arr[count] += line.charAt(i);
            }
        }
        return arr;
    }

    private static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private boolean joinNode(String port, Socket socket) {
        String avdNumber = Integer.toString(Integer.parseInt(port) / 2);
        String hashID;
        try {
            hashID = genHash(avdNumber);
        } catch (NoSuchAlgorithmException e) {
            hashID = null;
            e.printStackTrace();
            return false;
        }
        Node insert = null;
        String predecessor, successor, pPort, sPort;
        predecessor = successor = pPort = sPort = null;

        if (nodeList.size() == 1) {
            Node node = nodeList.get(0);
            node.predecessorID = hashID;
            node.successorID = hashID;
            node.predecessorPort = avdNumber;
            node.successorPort = avdNumber;
            predecessorPort = successorPort = avdNumber;
            insert = new Node(avdNumber, hashID, node.node, node.node, node.avdNumber, node.avdNumber);
            if (hashID.compareTo(node.node) >= 0) {
                nodeList.add(insert);
            } else {
                nodeList.add(0, insert);
            }
            predecessor = successor = node.node;
            pPort = sPort = node.avdNumber;

        } else {
            for (int i = 0; i < nodeList.size(); i++) {
                Node node = nodeList.get(i);
                if (i == nodeList.size() - 1) {
                    Node prevNode = nodeList.get(i - 1);
                    if (hashID.compareTo(node.node) <= 0 &&
                            (node.predecessorID == null || hashID.compareTo(node.predecessorID) >= 0)) {
                        // Insert before node
                        prevNode.successorPort = avdNumber;
                        prevNode.successorID = hashID;
                        predecessor = node.predecessorID;
                        successor = node.node;
                        pPort = node.predecessorPort;
                        sPort = node.avdNumber;
                        insert = new Node(avdNumber, hashID, predecessor, successor, pPort, sPort);
                        nodeList.add(i, insert);
                        node.predecessorID = hashID;
                        node.predecessorPort = avdNumber;
                        break;
                    } else {
                        // Add to the end
                        predecessor = node.node;
                        successor = node.successorID;
                        pPort = node.avdNumber;
                        sPort = node.successorPort;
                        insert = new Node(avdNumber, hashID, predecessor, successor, pPort, sPort);
                        node.successorID = hashID;
                        node.successorPort = avdNumber;
                        nodeList.add(insert);
                        nodeList.get(0).predecessorID = hashID;
                        nodeList.get(0).predecessorPort = avdNumber;
                        break;
                    }
                } else if (i == 0) {
                    if (hashID.compareTo(node.node) <= 0) {
                        // Insert into list before node
                        predecessor = node.predecessorID;
                        successor = node.node;
                        pPort = node.predecessorPort;
                        sPort = node.avdNumber;
                        insert = new Node(avdNumber, hashID, predecessor, successor, pPort, sPort);
                        nodeList.add(i, insert);
                        nodeList.get(nodeList.size() - 1).successorPort = avdNumber;
                        nodeList.get(nodeList.size() - 1).successorID = hashID;
                        node.predecessorID = hashID;
                        node.predecessorPort = avdNumber;
                        break;
                    }
                } else if (hashID.compareTo(node.node) <= 0 &&
                        (node.predecessorID == null || hashID.compareTo(node.predecessorID) >= 0)) {
                    Node prevNode = nodeList.get(i - 1);
                    // Insert into list before node
                    prevNode.successorID = hashID;
                    prevNode.successorPort = avdNumber;
                    predecessor = node.predecessorID;
                    successor = node.node;
                    pPort = node.predecessorPort;
                    sPort = node.avdNumber;
                    insert = new Node(avdNumber, hashID, predecessor, successor, pPort, sPort);
                    nodeList.add(i, insert);
                    node.predecessorID = hashID;
                    node.predecessorPort = avdNumber;
                    break;
                }
            }
        }

        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(predecessor + "," + successor + "," + pPort + "," + sPort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Node node : nodeList) {
            Log.d(TAG, "NodeID: " + node.node + " PredecessorID: " + node.predecessorID + " SuccessorID: " + node.successorID);
            Log.d(TAG, "AVD: " + node.avdNumber + " PredecessorPort: " + node.predecessorPort + " SuccessorPort: " + node.successorPort);
        }
        Log.d(TAG, "\n");

        return true;
    }

    private String findCorrectAvd(String selection) {
        if (selection == null) {
            Log.e(TAG, "findCorrectAvd: Selection was null!");
        }
        Socket socket = null;
        String line = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(JOIN_PORT));
            // socket.setSoTimeout(TIMEOUT_DELAY);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(FIND_AVD + "|" + localPort + "|" + selection + "|SELECTION");

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            line = in.readLine();
            out.println(SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line;
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String avdNumber = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        localPort = Integer.toString(Integer.parseInt(avdNumber) * 2);
        predecessorID = successorID = null;
        predecessorPort = successorPort = null;
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            if (!localPort.equals(JOIN_PORT)) {
                // Request join with port 5554
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, JOIN, null);
            } else {
                Node newNode = new Node(avdNumber, genHash(avdNumber), predecessorID, successorID, predecessorPort, successorPort);
                nodeList.add(newNode);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            Log.e(TAG, "Failed to create server socket");
            e.printStackTrace();
        }

        return false;
    }

    private void deleteSelection(String selection) {
        String[] files = getContext().fileList();
        String dir = getContext().getFilesDir().getAbsolutePath();
        for (String file : files) {
            if (selection.equals(file)) {
                // Delete the file
                File f0 = new File(dir, file);
                f0.delete();
                break;
            }
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        String[] files = getContext().fileList();
        String dir = getContext().getFilesDir().getAbsolutePath();
        Socket socket = null;
        if (selection.equals("@")) { // Local
            for (String file : files) {
                // Delete the file
                File f0 = new File(dir, file);
                f0.delete();
            }
            return 1;
        } else if (selection.equals("*") && predecessorPort != null) { // Whole DMT
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(JOIN_PORT));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(DELETE_ALL + "|" + localPort + "|" + null + "|" + null);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return 1;
        } else if (predecessorPort == null) {
            deleteSelection(selection);
        } else {


            // Find the correct AVD
            String avd = findCorrectAvd(selection);
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(avd));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(DELETE_SELECTION + "|" + localPort + "|" + selection + "|" + null);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public Uri insertSelection(Uri uri, ContentValues values) {

        String fileName = (String) values.get("key");
        String value = (String) values.get("value");
        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }
        return uri;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = (String) values.get("key");
        String value = (String) values.get("value");
        try {
            String avd;
            if (predecessorPort == null) {
                avd = localPort;
            } else {
                avd = findCorrectAvd(key);
            }
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(avd));
                // socket.setSoTimeout(TIMEOUT_DELAY);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(INSERT_SELECTION + "|" + localPort + "|" + key + "|" + value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
            e.printStackTrace();
        }
        return uri;
    }

    private MatrixCursor localQuery() {
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        try {
            String[] files = getContext().fileList();
            for (String file : files) {
                FileInputStream fis = getContext().openFileInput(file);
                InputStreamReader isr = new InputStreamReader(fis);
                BufferedReader reader = new BufferedReader(isr);
                StringBuilder sb = new StringBuilder();

                String line = "";
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                String val = sb.toString();
                cursor.newRow().add("key", file).add("value", val);
                fis.close();
            }
        } catch (IOException e) {
            Log.e(TAG, "Open file failed on query");
        }
        return cursor;
    }

    private String queryAll() {
        String line = "";
        MatrixCursor cursor = localQuery();
        while (cursor.moveToNext()) {
            line += cursor.getString(cursor.getColumnIndex("key")) + ",";
            line += cursor.getString(cursor.getColumnIndex("value")) + ",";
        }
        return line;
    }

    private String querySelection(String selection) {
        String line = null;
        try {
            FileInputStream fis = getContext().openFileInput(selection);
            InputStreamReader isr = new InputStreamReader(fis);
            BufferedReader reader = new BufferedReader(isr);
            StringBuilder sb = new StringBuilder();

            line = "";
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }

            String val = sb.toString();
            fis.close();
            line = selection + "," + val;
        } catch (IOException e) {
            Log.e(TAG, "Open file failed on query");
        }
        return line;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        if (predecessorPort == null) {
            if (selection.equals("@") || selection.equals("*")) {
                cursor = localQuery();
            } else {
                try {
                    FileInputStream fis = getContext().openFileInput(selection);
                    InputStreamReader isr = new InputStreamReader(fis);
                    BufferedReader reader = new BufferedReader(isr);
                    StringBuilder sb = new StringBuilder();

                    String line = "";
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }

                    String val = sb.toString();
                    cursor.newRow().add("key", selection).add("value", val);
                    fis.close();
                } catch (IOException e) {
                    Log.e(TAG, "Open file failed on query");
                }
                return cursor;
            }
        } else {
            Socket socket = null;
            String line = null;
            if (selection.equals("@")) { // Local query
                cursor = localQuery();
            } else if (selection.equals("*")) { // Query entire DHT
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(JOIN_PORT));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(QUERY_ALL + "|" + localPort + "|" + null + "|" + null);
                    Log.e(TAG, "Request sent.");

                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    line = in.readLine();
                    Log.e(TAG, "query: Received back");
                    String[] strs = line.split(",");
                    if (strs.length < 2) {
                        Log.e(TAG, "queryAll: invalid content values count");
                    }
                    for (int i = 0; i < strs.length; i += 2) {
                        cursor.newRow().add("key", strs[i]).add("value", strs[i + 1]);
                    }

                    // out.println(SUCCESS);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                String avd = findCorrectAvd(selection);
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(avd));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(QUERY_SELECTION + "|" + localPort + "|" + selection + "|" + null);

                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    line = in.readLine();

                    String[] lines = line.split(",");
                    cursor.newRow().add("key", lines[0]).add("value", lines[1]);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String clientPort = null;
            Socket client = null;
            try {
                while (true) {
                    // Read in initial message from user
                    client = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    String msg = in.readLine();
                    String[] params = splitInput(msg);
                    String requestType = params[0];
                    clientPort = params[1];
                    Log.e(TAG, "doInBackground REQUEST_TYPE: " + requestType);
                    // Add new node to the stuff yo
                    if (requestType.equals(JOIN) && localPort.equals(JOIN_PORT)) {
                        joinNode(clientPort, client);
                    } else if (requestType.equals(INSERT_SELECTION)) {
                        ContentValues val = new ContentValues();
                        val.put("key", params[2]);
                        val.put("value", params[3]);
                        insertSelection(M_URI, val);
                    } else if (requestType.equals(FIND_AVD)) {
                        String selection = params[2];
                        selection = genHash(selection);
                        int port = 0;
                        for (int i = 0; i < nodeList.size(); i++) {
                            Node node = nodeList.get(i);
                            if (i == 0) {
                                if ((node.predecessorID == null || node.successorID == null)
                                        || node.node.compareTo(selection) >= 0) {
                                    port = Integer.parseInt(node.avdNumber) * 2;
                                    break;
                                }
                            } else if ((node.predecessorID == null || node.successorID == null)
                                    || node.predecessorID.compareTo(selection) < 0 &&
                                    node.node.compareTo(selection) >= 0) {
                                port = Integer.parseInt(node.avdNumber) * 2;
                                break;
                            } else if (i == nodeList.size() - 1) {
                                port = Integer.parseInt(node.successorPort) * 2;
                                break;
                            }
                        }
                        Log.e(TAG, "Port for: " + selection + " = " + port);
                        out.println(Integer.toString(port));

                        String test = in.readLine();
                    } else if (requestType.equals(DELETE_ALL)) {
                        for (Node node : nodeList) {
                            int port = Integer.parseInt(node.avdNumber) * 2;
                            Socket socket = null;
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                            PrintWriter o = new PrintWriter(socket.getOutputStream(), true);
                            o.println(DELETE_YOUR_VALUES + "|" + localPort + "|" + null + "|" + null);

                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String test = in.readLine();

                            socket.close();
                        }
                    } else if (requestType.equals(DELETE_YOUR_VALUES)) {
                        delete(M_URI, "@", null);
                        out.println(SUCCESS);
                    } else if (requestType.equals(DELETE_SELECTION)) {
                        String selection = params[2];
                        deleteSelection(selection);
                    } else if (requestType.equals(QUERY_ALL)) {
                        String line = "";
                        for (Node node : nodeList) {
                            int port = Integer.parseInt(node.avdNumber) * 2;
                            if (node.avdNumber.equals("5554")) {
                                if (line == null) {
                                    line = queryAll();
                                } else {
                                    line += queryAll();
                                }
                            } else {
                                Socket socket = null;
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                                PrintWriter o = new PrintWriter(socket.getOutputStream(), true);
                                o.println(QUERY_YOUR_VALUES + "|" + localPort + "|" + null + "|" + null);

                                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                if (line == null) {
                                    line = in.readLine();
                                } else {
                                    line += in.readLine();
                                }
                                Log.e(TAG, "Finished: " + node.avdNumber);
                                socket.close();
                            }
                        }
                        out.println(line);
                    } else if (requestType.equals(QUERY_YOUR_VALUES)) {
                        out.println(queryAll());
                    } else if (requestType.equals(QUERY_SELECTION)) {
                        String selection = params[2];
                        String line = querySelection(selection);
                        out.println(line);
                    }
                }
            } catch (SocketTimeoutException e) {
                Log.e(TAG, "Socket timed out");
                closeSocket(client);
            } catch (SocketException e) {
                Log.e(TAG, "Socket exception");
                closeSocket(client);
            } catch (IOException e) {
                Log.e(TAG, "IOException");
                closeSocket(client);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            closeSocket(client);
            return null;
        }

        private void closeSocket(Socket socket) {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(String... messages) {

            // ((TextView) findViewById(R.id.textView1)).append(text);
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket = null;
            String requestType = msgs[0];
            if (requestType.equals("JOIN")) {
                requestJoin();
            } else if (requestType.equals(DELETE_ALL)) {
                String key = msgs[1];
                String value = msgs[2];
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successorPort) * 2);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(DELETE_ALL + "|" + localPort + "|" + "*" + "|" + null);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                if (socket != null) socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        private boolean requestJoin() {
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(JOIN_PORT));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(JOIN + "|" + localPort + "|" + " " + "| ");

                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String line = in.readLine();
                if (line != null) {
                    String[] response = line.split(",");
                    if (response.length >= 2) {
                        predecessorID = response[0];
                        successorID = response[1];
                        predecessorPort = response[2];
                        successorPort = response[3];
                    }
                } else {
                    predecessorID = successorID = successorPort = predecessorPort = null;
                }

                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }

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
