package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.preference.PreferenceManager;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    DBHelper helper;
    private SQLiteDatabase sqlDB;
    public static String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    public static final Uri CONTENT_URI = Uri.parse("content://" + AUTHORITY);
    public static String ret = "";
    public static String ret2 = "";
    public static String keyHash = "";
    public static MatrixCursor mc;
    public static int testSize = 0;
    public static HashMap<String, String> finalMap;
    public static TreeMap<String, Integer> nodeList;
    public static ArrayList<Integer> nList;
    public static String Tag = "SimpleDynamo ";
    public static int successor;
    public static int successor2;
    public static int predecessor;
    public static int predecessor2;
    public static int myNode;
    public static String myPort;
    SharedPreferences prefs = null;
    Object lock = new Object();
    Object lock2 = new Object();
    Object lock3 = new Object();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        String whereClause = "key" + "=?";
        String[] whereArgs = new String[]{String.valueOf(selection)};
        sqlDB.delete(DBHelper.TABLE_NAME, whereClause, whereArgs);


        int coordinator = findCoordinator(selection);

        Thread t = new Thread(new deleteThread(selection, coordinator));
        t.start();

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String[] cv = new String[4];
        cv[0] = (String) values.get("key");
        cv[1] = (String) values.get("value");

        if (!values.containsKey("version")) {
            // First time the key is coming, so versioned with 0
            String s = 0 + "";
            values.put("version", s);
            cv[2] = s;
        } else {
            cv[2] = (String) values.get("version");
        }

        Log.d(Tag, "Received Insert, key:" + cv[0] + " value: " + cv[1] + " version: " + cv[2]);

        int coordinator = findCoordinator(cv[0]);

        Log.d(Tag, "key: " + cv[0] + " belongs to " + coordinator);


        if (values.containsKey("recovery")) {
            values.remove("recovery");
            long row = sqlDB.insertWithOnConflict("myTable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
            if (row > 0)
                return uri;
        }


        if (coordinator == myNode) {

            Cursor cursor = sqlDB.rawQuery("Select key, value, version from myTable where key = '" + cv[0] + "'", null);
            if (cursor.getCount() != 0) {
                cursor.moveToFirst();
                int oldVersion = Integer.parseInt(cursor.getString(cursor.getColumnIndex("version")));
                values.remove("version");
                values.put("version", (oldVersion + 1) + "");
                Log.d(Tag, "******* Key Exists, version updated: " + oldVersion + 1);
            }

            long row = sqlDB.insertWithOnConflict("myTable", null, values, SQLiteDatabase.CONFLICT_REPLACE);

            if (row > 0) {
                Log.d(Tag, "local insert success, key: " + cv[0] + ", replicating now");
                Thread t = new Thread(new replicate(cv));
                t.start();
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Log.d(Tag, "******Interrupted while sendToSuccessors" + e.toString());
                }
                Log.d(Tag, "key: " + cv[0] + " replication complete");
                return uri;
            } else return null; //______ERROR if returned null______

        }

        if (values.containsKey("insertfailure")) {
            values.remove("insertfailure");
            Cursor cursor = sqlDB.rawQuery("Select key, value, version from myTable where key = '" + cv[0] + "'", null);
            if (cursor.getCount() != 0) {
                cursor.moveToFirst();
                int oldVersion = Integer.parseInt(cursor.getString(cursor.getColumnIndex("version")));
                values.remove("version");
                values.put("version", (oldVersion + 1) + "");
                Log.d(Tag, "******* Key Exists, version updated: " + oldVersion + 1);
            }

            long row = sqlDB.insertWithOnConflict("myTable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
            if (row > 0)
                return uri;

        }

        if (values.containsKey("replicate")) {
            values.remove("replicate");
            Cursor cursor = sqlDB.rawQuery("Select key, value, version from myTable where key = '" + cv[0] + "'", null);
            if (cursor.getCount() != 0) {
                cursor.moveToFirst();
                int oldVersion = Integer.parseInt(cursor.getString(cursor.getColumnIndex("version")));
                values.remove("version");
                values.put("version", (oldVersion + 1) + "");
                Log.d(Tag, "******* Key Exists, version updated: " + oldVersion + 1);
            }

            long row = sqlDB.insertWithOnConflict("myTable", null, values, SQLiteDatabase.CONFLICT_REPLACE);
            if (row > 0)
                return uri;
        }

        Thread t = new Thread(new sendToCoordinator(cv, coordinator));

        t.start();

        try {

            t.join();

        } catch (InterruptedException e) {

            Log.d(Tag, "******Interrupted while sendToCoordinator" + e.toString());
        }

        return uri;
    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myNode = Integer.parseInt(portStr);

        nodeList = new TreeMap<String, Integer>(
                new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.compareTo(o2);
                    }
                });

        try {
            nodeList.put(genHash(Integer.toString(5554)), 5554);
            nodeList.put(genHash(Integer.toString(5556)), 5556);
            nodeList.put(genHash(Integer.toString(5558)), 5558);
            nodeList.put(genHash(Integer.toString(5560)), 5560);
            nodeList.put(genHash(Integer.toString(5562)), 5562);
//            nodeHash = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            Log.d(Tag, " Error with genhash()");
        }
        nList = new ArrayList<Integer>();
        nList.addAll(nodeList.values());

        //___________get Successor Predecessor for this node___________
        for (int i = 0; i < nList.size(); i++) {
            if (nList.get(i) == myNode) {
                if (i == 0) {
                    successor = nList.get(1);
                    successor2 = nList.get(2);
                    predecessor = nList.get(4);
                    predecessor2 = nList.get(3);
                    break;
                }
                if (i == nList.size() - 1) {
                    successor = nList.get(0);
                    successor2 = nList.get(1);
                    predecessor = nList.get(i - 1);
                    predecessor2 = nList.get(i - 2);
                    break;
                }
                if (i > 0 && i < nList.size()) {
                    successor = nList.get(i + 1);
                    if (i != 3)
                        successor2 = nList.get(i + 2);
                    else
                        successor2 = nList.get(0);
                    predecessor = nList.get(i - 1);
                    if (i != 1)
                        predecessor2 = nList.get(i - 2);
                    else
                        predecessor2 = nList.get(nList.size() - 1);
                    break;
                }
            }
        }


        helper = new DBHelper(getContext());
        sqlDB = helper.getWritableDatabase();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.d(Tag, "Error *** Can't create a ServerSocket " + e.toString());
            //return;
        }


        prefs = PreferenceManager.getDefaultSharedPreferences(getContext());

        if (prefs.getBoolean("firstrun", true)) {
            prefs.edit().putBoolean("firstrun", false).commit();
        } else {
            Thread t = new Thread(new recoveryThread());
            t.start();
        }

        if (sqlDB == null)
            return false;
        else
            return true;

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {

        Log.d(Tag, "Query - " + selection);
        if (selection.contains("@")) {
            synchronized (lock2) {
                Cursor cursor = sqlDB.rawQuery("Select key, value from myTable", null);
                Log.d(Tag, "No. of rows ret: " + Integer.toString(cursor.getCount()));
                return cursor;
            }
        } else if (selection.contains("*")) {

            Thread t = new Thread(new queryStarThread());
            t.start();

            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //Log.d("!!!!!!TEST SIZE!!!!!!", Integer.toString(testSize));

            MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});

            Iterator it = finalMap.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                mc.newRow().add(pair.getKey()).add(pair.getValue());
            }

            return mc;


        } else if (selection.equals("r")) {
            Cursor cursor = sqlDB.rawQuery("Select key, value, version from myTable", null);
            return cursor;
        } else if ("q".equals(sortOrder)) {
            Cursor cursor = sqlDB.rawQuery("Select key, value from myTable where key = '" + selection + "'", null);

            return cursor;

        } else if ("f".equals(sortOrder)) {
            Cursor cursor = sqlDB.rawQuery("Select key, value, version from myTable where key = '" + selection + "'", null);

            return cursor;

        }

        int coordinator = findCoordinator(selection);
        Log.d(Tag, "key: " + selection + " is in " + coordinator + "");

        if (coordinator == myNode) {

            synchronized (lock) {

                Cursor cursor = sqlDB.rawQuery("Select key, value, version from myTable where key = '" + selection + "'", null);
                //Log.d(Tag, "No. of rows ret: " + Integer.toString(cursor.getCount()));
                if ((cursor.getCount()) != 0) {
                    cursor.moveToFirst();
//                    String finalValue = "";
//                    int v1 = Integer.parseInt(cursor.getString(cursor.getColumnIndex("version")));
//                    int v2 = Integer.parseInt(ret.split("\\.")[1]);
//
//
//                    if (v1 > v2) {
//                        finalValue = cursor.getString(cursor.getColumnIndex("value"));
//                    } else {
//                        finalValue = ret.split("\\.")[0];
//                    }
                    MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
                    mc.newRow().add(selection).add(cursor.getString(cursor.getColumnIndex("value")));
                    return mc;

                } else {
                    int succ = findSuccessor(coordinator);
                    int succ2 = findSuccessor2(coordinator);
                    Thread t = new Thread(new finalQuery(succ, succ2, selection));
                    t.start();
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        Log.d(Tag, e.toString());
                    }

                    MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
                    mc.newRow().add(selection).add(ret2.split("\\.")[0]);
                    return mc;

                }
            }
            // return cursor;
        } else {

           synchronized (lock3) {

                Log.d(Tag, "Query thread request");
                Thread t = new Thread(new queryThread(selection, coordinator));
                t.start();
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Log.d(Tag, e.toString());
                }
                Log.d(Tag, "Query thread Return");
                MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
                mc.newRow().add(selection).add(ret);
                Log.d(Tag, "Returned Value: " + ret);
                return mc;
            }
        }

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
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

    //______For finding the coordinator of a given key______
    public int findCoordinator(String s) {


        TreeMap<String, Integer> nodeList = new TreeMap<String, Integer>(
                new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.compareTo(o2);
                    }
                });

        try {
            nodeList.put(genHash(Integer.toString(5554)), 5554);
            nodeList.put(genHash(Integer.toString(5556)), 5556);
            nodeList.put(genHash(Integer.toString(5558)), 5558);
            nodeList.put(genHash(Integer.toString(5560)), 5560);
            nodeList.put(genHash(Integer.toString(5562)), 5562);
            keyHash = genHash(s);

        } catch (NoSuchAlgorithmException e) {
            Log.d(Tag, " Error with genhash()");
        }
        // Find the node to which the key belongs (Coordinator)
        nodeList.put(keyHash, 1);
        ArrayList<Integer> al = new ArrayList<Integer>();
        al.addAll(nodeList.values());
        nodeList.remove(keyHash);
        int search = 1;
        int coordinator = 0;
        for (int h = 0; h < al.size(); h++) {
            if (al.get(h) == search) {
                if (h == al.size() - 1) {
                    coordinator = al.get(0);
                } else
                    coordinator = al.get(h + 1);
                break;
            }
        }
        return coordinator;
    }

    int findSuccessor(int coord) {
        int successor = 0;
        for (int i = 0; i < nList.size(); i++) {
            if (nList.get(i) == coord) {
                if (i == 0) {
                    successor = nList.get(1);
                    break;
                }
                if (i == nList.size() - 1) {
                    successor = nList.get(0);

                    break;
                }
                if (i > 0 && i < nList.size()) {
                    successor = nList.get(i + 1);
                    break;
                }
            }
        }
        return successor;
    }

    int findSuccessor2(int coord) {
        int successor2 = 0;
        for (int i = 0; i < nList.size(); i++) {
            if (nList.get(i) == coord) {
                if (i == 0) {
                    successor2 = nList.get(2);
                    break;
                }
                if (i == nList.size() - 1) {
                    successor2 = nList.get(1);
                    break;
                }
                if (i > 0 && i < nList.size()) {
                    if (i != 3)
                        successor2 = nList.get(i + 2);
                    else
                        successor2 = nList.get(0);
                    break;
                }
            }
        }
        return successor2;
    }

    //________________THREAD CLASS DEFINITIONS________________
    public class replicate implements Runnable {

        String key, value, version, toSend;

        public replicate(String[] cv) {
            this.key = cv[0];
            this.value = cv[1];
            this.version = cv[2];
            this.toSend = "replicate" + "." + key + "." + value + "." + version;
        }

        @Override
        public void run() {

            DataOutputStream out1, out2, out;
            DataInputStream in;
            try {
                Log.d(Tag, "send to successor1:" + successor + ", key: " + key);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successor * 2);
                socket.setSoTimeout(2000);
                in = new DataInputStream(socket.getInputStream());
                out1 = new DataOutputStream(socket.getOutputStream());
                out1.writeUTF(toSend);

                String reply = in.readUTF();

                if (reply == "A") {
                    Log.d("?|?|?|?|?|?", "YAYAYAYAYAYAY");
                }

                socket.close();

            } catch (UnknownHostException e) {
                Log.d(Tag, "Error sending to successor " + e.toString());
            } catch (IOException e) {
                Log.d(Tag, "Error sending to successor " + e.toString());
            }
            Boolean flag = false;
            try {

                Log.d(Tag, "send to successor2:" + successor2 + ", key: " + key);
                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successor2 * 2);
                socket2.setSoTimeout(2000);
                in = new DataInputStream(socket2.getInputStream());
                out2 = new DataOutputStream(socket2.getOutputStream());
                out2.writeUTF(toSend);
                String reply = in.readUTF();

                if (reply == "A") {
                    Log.d("?|?|?|?|?|?", "YAYAYAYAYAYAY");
                }

                socket2.close();

            } catch (UnknownHostException e) {
                Log.d(Tag, "Error sending to successor " + e.toString());
                flag = true;
            } catch (IOException e) {
                Log.d(Tag, "Error sending to successor " + e.toString());
                flag = true;
            } catch (Exception e) {
                flag = true;
            }

//            if (flag) {
//
//                Log.d(Tag, "Sending to successor2 failure");
//
//                int toNode = findSuccessor(SimpleDynamoActivity.successor2);
//                toSend = "insertfailure" + "." + key + "." + value + "." + version;
//                try {
//                    Log.d(Tag, "send to successor:" + toNode + " of successor:" + SimpleDynamoActivity.successor2 + ", key: " + key);
//                    Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), toNode * 2);
//
//                    out = new DataOutputStream(socket3.getOutputStream());
//
//                    out.writeUTF(toSend);
//
//                    socket3.close();
//
//                } catch (UnknownHostException e) {
//                    Log.d(Tag, "Error sending to coordinator " + e.toString());
//
//                } catch (IOException e) {
//                    Log.d(Tag, "Error sending to coordinator " + e.toString());
//
//                }
//
//
//            }


        }


    }

    public class sendToCoordinator implements Runnable {
        String key, value, version, toSend;
        int coordinator;

        public sendToCoordinator(String[] cv, int coord) {
            this.key = cv[0];
            this.value = cv[1];
            this.version = cv[2];
            this.toSend = "sendToCoordinator" + "." + key + "." + value + "." + version;
            this.coordinator = coord;
        }

        @Override
        public void run() {
            DataOutputStream out;
            DataInputStream in;
            Boolean flag = false;
            try {
                Log.d(Tag, "send to coordinator, key: " + key);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), coordinator * 2);
                socket.setSoTimeout(2000);
                out = new DataOutputStream(socket.getOutputStream());
                in = new DataInputStream(socket.getInputStream());
                out.writeUTF(toSend);

                String reply = in.readUTF();

                if (reply == "A") {
                    Log.d("?|?|?|?|?|?", "YAYAYAYAYAYAY");
                }

                socket.close();

            } catch (UnknownHostException e) {
                Log.d(Tag, "Error sending to coordinator " + e.toString());
                flag = true;
            } catch (IOException e) {
                Log.d(Tag, "Error sending to coordinator " + e.toString());
                flag = true;
            } catch (Exception e) {
                Log.d(Tag, "Error sending to coordinator " + e.toString());
                flag = true;
            }


            if (flag) {
                Log.d(Tag, "Sending to coordinator's successor1");

                int toNode = findSuccessor(coordinator);
                int toNode2 = findSuccessor2(coordinator);
                toSend = "insertfailure" + "." + key + "." + value + "." + version;
                try {
                    Log.d(Tag, "send to coordinator, key: " + key);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), toNode * 2);

                    out = new DataOutputStream(socket.getOutputStream());
                    in = new DataInputStream(socket.getInputStream());
                    out.writeUTF(toSend);

                    socket.close();
                    Log.d(Tag, "Sending to coordinator's successor2");
                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), toNode2 * 2);

                    out = new DataOutputStream(socket2.getOutputStream());
                    in = new DataInputStream(socket2.getInputStream());
                    out.writeUTF(toSend);

                    socket2.close();
                } catch (UnknownHostException e) {
                    Log.d(Tag, "Error sending to coordinator " + e.toString());

                } catch (IOException e) {
                    Log.d(Tag, "Error sending to coordinator " + e.toString());

                }

            }

        }
    }

    public class queryThread implements Runnable {
        String key;
        int coordinator;

        public queryThread(String k, int p) {
            this.key = k;
            this.coordinator = p;
        }

        @Override
        public synchronized void run() {
            DataOutputStream out1;
            DataInputStream in;
            String toSend = "query" + "." + key;
            Boolean flag = false;
            try {
                Log.d(Tag, "Inside Query thread for key: " + key);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), coordinator * 2);
                socket.setSoTimeout(2000);
                out1 = new DataOutputStream(socket.getOutputStream());
                out1.writeUTF(toSend);

                in = new DataInputStream(socket.getInputStream());
                ret = in.readUTF();

                socket.close();

            } catch (UnknownHostException e) {
                Log.d(Tag, "Error sending to successor " + e.toString());
                flag = true;
            } catch (IOException e) {
                Log.d(Tag, "Error sending to successor " + e.toString());
                flag = true;
            } catch (Exception e) {
                flag = true;
            }

            if (flag) {
                try {
                    String toSend2 = "queryfailure" + "." + key;
                    int toNode = findSuccessor(coordinator);
                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), toNode * 2);

                    out1 = new DataOutputStream(socket2.getOutputStream());
                    out1.writeUTF(toSend2);

                    in = new DataInputStream(socket2.getInputStream());
                    ret = in.readUTF();

                    socket2.close();


                } catch (UnknownHostException e) {
                    Log.d(Tag, "Error sending to successor " + e.toString());
                } catch (IOException e) {
                    Log.d(Tag, "Error sending to successor " + e.toString());
                }


            }

        }
    }


    public class finalQuery implements Runnable {

        int succ1, succ2;
        String key;

        public finalQuery(int s, int s2, String sel) {
            this.succ1 = s;
            this.succ2 = s2;
            this.key = sel;
        }

        @Override
        public synchronized void run() {
            DataOutputStream out1, out2;
            DataInputStream in1, in2;
            String toSend = "finalQuery" + "." + key;
            Boolean flag1 = false;
            Boolean flag2 = false;

            String reta = " ", retb = "";

            try {
                Log.d(Tag, "Inside finalQuery  " + succ1);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), succ1 * 2);
                socket.setSoTimeout(2000);
                out1 = new DataOutputStream(socket.getOutputStream());
                out1.writeUTF(toSend);

                in1 = new DataInputStream(socket.getInputStream());
                reta = in1.readUTF();
                Log.d(Tag, "Inside finalQuery   " + reta);
                socket.close();

            } catch (UnknownHostException e) {
                Log.d(Tag, "Error sending to successor1 " + e.toString());
                flag1 = true;
            } catch (IOException e) {
                Log.d(Tag, "Error sending to successor1 " + e.toString());
                flag1 = true;
            } catch (Exception e) {
                flag1 = true;
            }

            try {
                Log.d(Tag, "Inside finalQuery   " + succ2);
                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), succ2 * 2);
                socket2.setSoTimeout(2000);
                out2 = new DataOutputStream(socket2.getOutputStream());
                out2.writeUTF(toSend);

                in2 = new DataInputStream(socket2.getInputStream());
                retb = in2.readUTF();
                Log.d(Tag, "Inside finalQuery   " + retb);
                socket2.close();

            } catch (UnknownHostException e) {
                Log.d(Tag, "Error sending to successor2 " + e.toString());
                flag2 = true;
            } catch (IOException e) {
                Log.d(Tag, "Error sending to successor2 " + e.toString());
                flag2 = true;
            } catch (Exception e) {
                flag2 = true;
            }

            if (flag1) {
                ret2 = retb;
            } else if (flag2) {
                ret2 = reta;
//            } else {
//                int v1, v2;
//
//                v1 = Integer.parseInt(ret1.split("\\.")[1]);
//                v2 = Integer.parseInt(ret2.split("\\.")[1]);
//
//                if (v1 > v2) {
//                    ret = ret1;
//                } else if (v2 > v1) {
//                    ret = ret2;
//                } else
//                    ret = ret1;


            }

        }
    }


    public class deleteThread implements Runnable {

        String key;
        int coordinator;

        public deleteThread(String k, int c) {
            this.coordinator = c;
            this.key = k;


        }

        @Override
        public void run() {
            DataOutputStream out1;
            DataInputStream in;
            String toSend = "delete" + "." + key;
            Boolean flag = false;
            for (int i = 0; i < nList.size(); i++) {
                try {
                    int port = nList.get(i);
                    Log.d(Tag, "Inside delete thread for key: " + key);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port * 2);
                    socket.setSoTimeout(2000);
                    out1 = new DataOutputStream(socket.getOutputStream());
                    out1.writeUTF(toSend);
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.d(Tag, "Error sending to successor " + e.toString());
                    flag = true;
                } catch (IOException e) {
                    Log.d(Tag, "Error sending to successor " + e.toString());
                    flag = true;
                } catch (Exception e) {
                    flag = true;
                }
            }
        }
    }

    public class queryStarThread implements Runnable {

        @Override
        public void run() {

            int port;
            finalMap = new HashMap<String, String>();
            Log.d(Tag, "Query Star thread");
            String toSend = "querystar.request";
            for (int i = 0; i < nList.size(); i++) {
                try {
                    DataOutputStream out;
                    DataInputStream in;


                    port = nList.get(i);

                    if (port == SimpleDynamoProvider.myNode) {

                        Cursor cursor = sqlDB.rawQuery("Select key, value from myTable", null);
                        cursor.moveToFirst();
                        while (!cursor.isAfterLast()) {
                            String key = cursor.getString(cursor.getColumnIndex("key"));
                            String value = cursor.getString(cursor.getColumnIndex("value"));
                            finalMap.put(key, value);


                            cursor.moveToNext();
                        }

                    } else {

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port * 2);
                        out = new DataOutputStream(socket.getOutputStream());
                        in = new DataInputStream(socket.getInputStream());

                        out.writeUTF(toSend);

                        String result = in.readUTF();


                        String[] pairs = result.split("-");

                        for (String pair : pairs) {

                            finalMap.put(pair.split("\\.")[0], pair.split("\\.")[1]);
                        }

                        socket.close();

                        //   testSize = finalMap.size();
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
                    Socket socket = serverSocket.accept();

                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

                    String input = dis.readUTF();
                    Log.d("????????????????", input);
                    String[] cv = input.split("\\.");

                    if (cv[0].equals("replicate") || cv[0].equals("sendToCoordinator")) {
                        ContentValues values = new ContentValues();
                        values.put("key", cv[1]);
                        values.put("value", cv[2]);
                        values.put("version", cv[3]);

                        if (cv[0].equals("replicate"))
                            values.put("replicate", "true");

                        getContext().getContentResolver().insert(CONTENT_URI, values);

                        // if (cv[0].equals("sendToCoordinator")) {
                        dos.writeUTF("A");
                        //}
                    }

                    if (cv[0].equals("query")) {
                        Log.d(Tag, "Received Query Request from another Node");
                        Cursor resultCursor = getContext().getContentResolver().query(CONTENT_URI, null,
                                cv[1], null, null);
                        resultCursor.moveToFirst();
                        dos.writeUTF(resultCursor.getString(resultCursor.getColumnIndex("value")));
                    }

                    if (cv[0].equals("querystar")) {
                        Log.d(Tag, "Received Query * Request");
                        Cursor resultCursor = getContext().getContentResolver().query(CONTENT_URI, null,
                                "\"@\"", null, null);
                        resultCursor.moveToFirst();
                        String result = "";
                        while (!resultCursor.isAfterLast()) {
                            String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
                            String value = resultCursor.getString(resultCursor.getColumnIndex("value"));

                            result = result + key + "." + value + "-";

                            resultCursor.moveToNext();
                        }
                        dos.writeUTF(result);

                    }

                    if (cv[0].equals("insertfailure")) {
                        ContentValues values = new ContentValues();
                        values.put("key", cv[1]);
                        values.put("value", cv[2]);
                        values.put("version", cv[3]);
                        values.put("insertfailure", "true");
                        getContext().getContentResolver().insert(CONTENT_URI, values);
                    }

                    if (cv[0].equals("recovery")) {
                        Log.d(Tag, "Received recovery Request");
                        Cursor resultCursor = getContext().getContentResolver().query(CONTENT_URI, null,
                                "r", null, null);
                        resultCursor.moveToFirst();
                        String result = "";
                        while (!resultCursor.isAfterLast()) {
                            String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
                            String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
                            String version = resultCursor.getString(resultCursor.getColumnIndex("version"));
                            result = result + key + "." + value + "." + version + "-";

                            resultCursor.moveToNext();
                        }
                        dos.writeUTF(result);
                    }

                    if (cv[0].equals("queryfailure")) {
                        Cursor resultCursor = getContext().getContentResolver().query(CONTENT_URI, null,
                                cv[1], null, "q");
                        resultCursor.moveToFirst();
                        dos.writeUTF(resultCursor.getString(resultCursor.getColumnIndex("value")));
                    }

                    if (cv[0].equals("finalQuery")) {
                        Cursor resultCursor = getContext().getContentResolver().query(CONTENT_URI, null,
                                cv[1], null, "f");
                        resultCursor.moveToFirst();
                        String s = "";
                        if (resultCursor.getCount() != 0) {
                            s = resultCursor.getString(resultCursor.getColumnIndex("value")) + "." + resultCursor.getString(resultCursor.getColumnIndex("version"));
                        } else {
                            s = "a" + "." + "-1";
                        }
                        dos.writeUTF(s);
                    }

                    if (cv[0].equals("delete")) {
                        String whereClause = "key" + "=?";
                        String[] whereArgs = new String[]{String.valueOf(cv[1])};
                        sqlDB.execSQL("delete from " + DBHelper.TABLE_NAME);
                        //getContext().getContentResolver().delete(CONTENT_URI, whereClause, whereArgs);
                    }
                }
            } catch (IOException ioe) {
                Log.d("IOException", ioe.toString());
            }
            Log.d(Tag, "*** SERVER TASK EXITED ***");
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0].trim();
            // setText(strReceived);
            //return;
        }
    }

    public class recoveryThread implements Runnable {
        @Override
        public void run() {
            synchronized (lock2) {
                DataOutputStream out1, out2;
                DataInputStream in, in2;
                String toSend = "recovery.request";
                try {
                    Log.d(Tag, "Inside recovery thread ");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), SimpleDynamoProvider.successor * 2);
                    out1 = new DataOutputStream(socket.getOutputStream());
                    out1.writeUTF(toSend);

                    in = new DataInputStream(socket.getInputStream());
                    String ret = in.readUTF();


                    String[] pairs = ret.split("-");

                    //if (pairs.length > 1) {
                    for (String pair : pairs) {
                        if (pair.split("\\.").length == 3) {
                            String key = pair.split("\\.")[0];
                            String value = pair.split("\\.")[1];
                            String version = pair.split("\\.")[2];
                            int coordinator = findCoordinator(key);

                            if (coordinator == SimpleDynamoProvider.myNode || coordinator == SimpleDynamoProvider.predecessor || coordinator == SimpleDynamoProvider.predecessor2) {

                                ContentValues values = new ContentValues();
                                values.put("key", key);
                                values.put("value", value);
                                values.put("version", version);
                                values.put("recovery", "true");
                                getContext().getContentResolver().insert(CONTENT_URI, values);
                            }

                        } //}
                    }
                    socket.close();
                    Log.d("1", "complete");
                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), SimpleDynamoProvider.predecessor * 2);
                    out2 = new DataOutputStream(socket2.getOutputStream());
                    out2.writeUTF(toSend);

                    in2 = new DataInputStream(socket2.getInputStream());
                    String ret2 = in2.readUTF();

                    String[] pairs2 = ret2.split("-");

                    // if (pairs2.length > 1) {
                    for (String pair : pairs2) {
                        if (pair.split("\\.").length == 3) {
                            String key = pair.split("\\.")[0];
                            String value = pair.split("\\.")[1];
                            String version = pair.split("\\.")[2];
                            int coordinator = findCoordinator(key);

                            if (coordinator == SimpleDynamoProvider.myNode || coordinator == SimpleDynamoProvider.predecessor || coordinator == SimpleDynamoProvider.predecessor2) {

                                ContentValues values = new ContentValues();
                                values.put("key", key);
                                values.put("value", value);
                                values.put("version", version);
                                values.put("recovery", "true");
                                getContext().getContentResolver().insert(CONTENT_URI, values);
                            }

                        }
                    }
                    socket2.close();
                    Log.d("2", "complete");
                } catch (UnknownHostException e) {
                    Log.d(Tag, "Error sending to successor " + e.toString());
                } catch (IOException e) {
                    Log.d(Tag, "Error sending to successor " + e.toString());
                }
            }
        }

    }
}




