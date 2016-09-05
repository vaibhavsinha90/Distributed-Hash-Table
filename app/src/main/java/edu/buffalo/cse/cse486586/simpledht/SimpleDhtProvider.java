package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.AbstractCursor;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String REMOTE_PORT0 = "11108";//54
    static final String REMOTE_PORT1 = "11112";//56
    static final String REMOTE_PORT2 = "11116";//58
    static final String REMOTE_PORT3 = "11120";//60
    static final String REMOTE_PORT4 = "11124";//62
    static final String[] ports={REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};

    static final int SERVER_PORT = 10000;
    static String DevId;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static String nodeID;
    static String predecessor="";
    static String successor="";
    static Integer joined_nodes=0;
    static boolean islast=false;
    static boolean isfirst=false;
    static TreeMap<String,Boolean> ActiveNodes = new TreeMap<String, Boolean>();
    static HashMap<String,String> hashTOport = new HashMap<String, String>();
    static HashMap<String,String> portTOhash = new HashMap<String, String>();
    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }
    public String findNode(String hashOfKey)
    {
        try
        {
            if(hashOfKey.compareTo(portTOhash.get(predecessor))>0 && hashOfKey.compareTo(nodeID)<=0)
            {
                System.out.println("Here: " + portTOhash.get(predecessor)+ " < "+hashOfKey+" . NodeID= "+nodeID);
                return nodeID;
            }else if(predecessor.equals(hashTOport.get(nodeID)))
            {
                System.out.println("Here: " + predecessor+ " = "+nodeID); //could have checked successor too. Same thing.
                return nodeID;
            }else if(hashOfKey.compareTo(nodeID)>0 && islast)
            {
                System.out.println("Here: " + hashOfKey+ " > "+nodeID+" . Also, this is the Last Node");
                return portTOhash.get(successor);
            }else if(hashOfKey.compareTo(nodeID)<=0 && isfirst)
            {
                return nodeID;
            }

            System.out.println("Nothing Worked. Will fwd to successor!");


            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
            Log.i(TAG,"HK:" +hashOfKey+" was sent to "+successor);
            Log.i(TAG, "HP: " + portTOhash.get(predecessor));
            Log.i(TAG, "HS: " + portTOhash.get(successor));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader r= new BufferedReader(new InputStreamReader(socket.getInputStream()));
            w.write("CheckKey*" + hashOfKey + "\n");
            w.flush();
            String rep=r.readLine();
            socket.close();
            return rep;
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem Finding CorrectNode!"+e);
        }

        return "SthWentWrong!";
    }

    public void MsgToCN_insert(String DestinationNodeID, ContentValues values)
    {
        try
        {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(hashTOport.get(DestinationNodeID)));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader r= new BufferedReader(new InputStreamReader(socket.getInputStream()));
            w.write("NewData*" + values.getAsString("key")+":"+values.getAsString("value") + "\n");
            w.flush();
            String rep=r.readLine();
            if(rep.equals("MsgRecvd"))
                Log.i(TAG,"Msg was recvd by CN.");
            else
                Log.e(TAG, "Msg was NOT recvd by CN. Reply=" + rep);
            socket.close();
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to CorrectNode!");
        }

    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String CorrectNode="@#$%";
        try
        {
            CorrectNode=findNode(genHash(values.getAsString("key")));
            Log.i(TAG, "CorrectNode=" + CorrectNode);
        }
        catch (Exception e)
        {
            Log.e(TAG,"Error finding correct node for insertion!");
        }
        if(CorrectNode.equals(nodeID)) {
            String filename = values.getAsString("key");
            String string = values.getAsString("value");
            FileOutputStream outputStream;

            try {
                outputStream = getContext().getApplicationContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
            Log.v("inserted locally", string + "->" + filename);
        }
        else
        {
            Log.i(TAG, "Fwding msg to Correct Node: "+CorrectNode);
            MsgToCN_insert(CorrectNode, values);
        }
        return uri;
    }
    public int insertHere(String key, String val) {

        String filename = key;
        FileOutputStream outputStream;

        try {
            outputStream = getContext().getApplicationContext().openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(val.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
            return 0;
        }
        Log.v("inserted", val + "->" + filename);
        return 1;
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        final String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        DevId=portStr;
        Log.i(TAG, "DevId: "+DevId);

        try
        {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (IOException e)
        {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        nodeID="DUMMY";
        try
        {
            nodeID = genHash(DevId);
            if(DevId.equals("5554"))
            {
                ActiveNodes.put(nodeID,true);
                hashTOport.put(nodeID,REMOTE_PORT0);
                portTOhash.put(REMOTE_PORT0,nodeID);
                Integer avdid=5554;
                for (int i = 1; i < ports.length; i++)
                {
                    avdid=avdid+2;
                    ActiveNodes.put(genHash(avdid.toString()), false);
                    hashTOport.put(genHash(avdid.toString()), ports[i]);
                    portTOhash.put(ports[i],genHash(avdid.toString()));
                }

                predecessor=hashTOport.get(nodeID);
                successor=predecessor;
            }
            else
            {
                Integer avdid=5552;
                for (int i = 0; i < ports.length; i++)
                {
                    avdid=avdid+2;
                    hashTOport.put(genHash(avdid.toString()), ports[i]);
                    portTOhash.put(ports[i],genHash(avdid.toString()));
                }
                predecessor=hashTOport.get(nodeID);
                successor=predecessor;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "JOIN*"+nodeID, myPort);
            }
        }
        catch (Exception e)
        {
            Log.e(TAG, "NodeID could not be generated!! Error: " + e);
            return false;
        }
        return true;
    }

    public String MsgToCN_query(String DestinationNodeID, String key)
    {
        try
        {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(hashTOport.get(DestinationNodeID)));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader r= new BufferedReader(new InputStreamReader(socket.getInputStream()));
            w.write("GetVal*" + key + "\n");
            w.flush();
            String val=r.readLine();
            Log.i(TAG, "Value for " + key + " = " + val);
            socket.close();
            return val;
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to CorrectNode!");
        }

        return "SthWentWrong!";

    }
    public String getSuccessor(String nextNode)
    {
        String suc="";
        try
        {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nextNode));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader r= new BufferedReader(new InputStreamReader(socket.getInputStream()));

            w.write("GetSuccessor" + "\n");
            w.flush();
            suc=r.readLine();
            socket.close();
            return suc;
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem getting Successor!");
        }

        return suc;

    }
    public HashMap<String,String> MsgToCN_allquery(String nextNode)
    {
        HashMap<String,String> KVpairs=new HashMap<String, String>();
        try
        {

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nextNode));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

            w.write("GetAllVal" + "\n");
            w.flush();
            ObjectInputStream i= new ObjectInputStream(socket.getInputStream());
            Object Extdata=i.readObject();
            w.write("Ack"+"\n");
            w.flush();
            KVpairs= (HashMap<String,String>) Extdata;
            socket.close();
            return KVpairs;
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to CorrectNode for all data!");
        }

        return KVpairs;

    }

    public String queryHere(String fname)
    {
        FileInputStream inputStream;
        long k;
        try {
            inputStream = getContext().getApplicationContext().openFileInput(fname);
            k = inputStream.getChannel().size();
            byte[] b = new byte[(int) k];
            inputStream.read(b);
            inputStream.close();
            String strval = new String(b);
            return strval;

        } catch (Exception e) {
            Log.e(TAG, "File read failed :X");
        }
        return "SthWentWrong!";
    }

    public HashMap<String,String> AllLocalData(String mode)
    {
        HashMap<String,String> KVpairs= new HashMap<String, String>();

        if(mode.equals("Query"))
        {
            Log.i(TAG,"Retrieving all content from this avd for *");
            FileInputStream inputStream;
            ArrayList selection_crit = new ArrayList();

            File dir = getContext().getApplicationContext().getFilesDir();
            File[] files = dir.listFiles();
            for (int i = 0; i < files.length; i++)
                selection_crit.add(files[i].getName());

            String fname, strval;
            long k;
            for (Object fname_o : selection_crit)
            {
                fname = fname_o.toString();
                try {
                    inputStream = getContext().getApplicationContext().openFileInput(fname);
                    k = inputStream.getChannel().size();
                    byte[] b = new byte[(int) k];
                    inputStream.read(b);
                    inputStream.close();
                    strval = new String(b);
                    KVpairs.put(fname, strval);
                    Log.v("query", fname + " " + strval);
                } catch (Exception e) {
                    Log.e(TAG, "File read failed :X");
                }

            }
        }else if(mode.equals("Delete"))
        {
            Log.i(TAG,"Deleting all content from this avd for *");
            ArrayList selection_crit = new ArrayList();

            File dir = getContext().getApplicationContext().getFilesDir();
            File[] files = dir.listFiles();
            for (int i = 0; i < files.length; i++)
                selection_crit.add(files[i].getName());

            String fname;
            int flag=0;
            for (Object fname_o : selection_crit)
            {
                fname = fname_o.toString();
                File file = new File(getContext().getApplicationContext().getFilesDir().getPath() + "/" + fname);
                boolean deleted = file.delete();
                if (deleted)
                    Log.i(TAG, "deleted " + fname + "!");
                else {
                    Log.e(TAG, "Error in deleting -AllLocalDataDelete!");
                    flag=-1;
                }

            }
            if(flag==0)
                KVpairs.put("Deleted","All");
            else
                KVpairs.put("Deleted","Some");

        }
        return KVpairs;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String[] colnames = {"key", "value"};
        MatrixCursor m = new MatrixCursor(colnames);
        FileInputStream inputStream;
        ArrayList selection_crit = new ArrayList();
        boolean specialQuery=false;

        if (selection.equals("@") || selection.equals("*"))
        {
            specialQuery=true;
            //Log.i(TAG,"SPCL QUERY!!");
            File dir=getContext().getApplicationContext().getFilesDir();
            File[] files = dir.listFiles();
            for (int i=0; i < files.length; i++)
                selection_crit.add(files[i].getName());
        }
        else
            selection_crit.add(selection);

        long k;
        String strval,fname;
        for (Object fname_o : selection_crit)
        {
            fname=fname_o.toString();
            if(specialQuery)
            {

                try {
                    inputStream = getContext().getApplicationContext().openFileInput(fname);
                    k = inputStream.getChannel().size();
                    byte[] b = new byte[(int) k];
                    inputStream.read(b);
                    inputStream.close();
                    strval = new String(b);
                    m.addRow(new Object[]{fname, strval});
                    Log.v("query", fname + " " + strval);
                } catch (Exception e) {
                    Log.e(TAG, "File read failed :X");
                }
            }
            else
            {
                String CN="";
                try
                {
                    CN=findNode(genHash(fname));
                    Log.i(TAG, "CorrectNode="+CN);
                }
                catch (Exception e)
                {
                    Log.e(TAG,"Error finding correct node for querying!");
                }
                if(CN.equals(nodeID)) {
                    try {
                        inputStream = getContext().getApplicationContext().openFileInput(fname);
                        k = inputStream.getChannel().size();
                        byte[] b = new byte[(int) k];
                        inputStream.read(b);
                        inputStream.close();
                        strval = new String(b);
                        m.addRow(new Object[]{fname, strval});
                        Log.v("query", fname + " " + strval);
                    } catch (Exception e) {
                        Log.e(TAG, "File read failed :X");
                    }
                }
                else
                {
                    String response=MsgToCN_query(CN, fname);
                    Log.i(TAG, "Recvd Query result from another avd. Val="+response);
                    m.addRow(new Object[]{fname, response});
                }
            }
        }

        if(selection.equals("*"))
        {

            String nextNode=successor;
            while(!nextNode.equals(hashTOport.get(nodeID)))
            {
                HashMap<String,String> response = MsgToCN_allquery(nextNode);
                Log.i(TAG, "Recvd Query result from another avd. # of Records: " + response.size());
                for(Map.Entry<String,String> KV : response.entrySet())
                {
                    m.addRow(new Object[]{KV.getKey(),KV.getValue()});
                }
                nextNode=getSuccessor(nextNode);
            }
        }

        return m;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        ArrayList selection_crit = new ArrayList();
        boolean specialQuery=false;
        if (selection.equals("@") || selection.equals("*"))
        {
            specialQuery=true;
            File dir=getContext().getApplicationContext().getFilesDir();
            File[] files = dir.listFiles();
            for (int i=0; i < files.length; i++)
                selection_crit.add(files[i].getName());
        }
        else
            selection_crit.add(selection);


        String fname;
        for (Object fname_o : selection_crit)
        {
            fname=fname_o.toString();
            if(specialQuery)
            {

                File file = new File(getContext().getApplicationContext().getFilesDir().getPath() + "/" + fname);
                boolean deleted = file.delete();
                if (deleted)
                    Log.i(TAG, "deleted " + fname + "!");
                else
                    Log.e(TAG, "Error in deleting -SQ!");
            }
            else
            {
                String CN="";
                try
                {
                    CN=findNode(genHash(fname));
                    Log.i(TAG, "CorrectNode="+CN);
                }
                catch (Exception e)
                {
                    Log.e(TAG,"Error finding correct node for deletion!");
                }
                if(CN.equals(nodeID)) {
                    File file = new File(getContext().getApplicationContext().getFilesDir().getPath() + "/" + fname);
                    boolean deleted = file.delete();
                    if (deleted)
                        Log.i(TAG, "deleted " + fname + "!");
                    else
                        Log.e(TAG, "Error in deleting -locally!");
                }
                else
                {
                    boolean response=MsgToCN_delete(CN, fname);
                    if (response)
                        Log.i(TAG, "Data was deleted at the CN");
                    else
                        Log.e(TAG, "Data was NOT deleted at the CN");

                }
            }
        }

        if(selection.equals("*"))
        {

            String nextNode=successor;
            while(!nextNode.equals(hashTOport.get(nodeID)))
            {
                boolean response = MsgToCN_alldelete(nextNode);
                Log.i(TAG, "Data at other avd deleted. t/f?: " + response);
                nextNode=getSuccessor(nextNode);
            }
        }

        return 0;
    }

    public boolean MsgToCN_alldelete(String nextNode)
    {
        try
        {

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nextNode));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader r= new BufferedReader(new InputStreamReader(socket.getInputStream()));
            w.write("DelAllVal" + "\n");
            w.flush();
            String reply=r.readLine();
            socket.close();
            if(reply.equals("Yes"))
                return true;
            else
                return false;
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to CorrectNode for all data deletion!");
        }

        return false;

    }

    public boolean MsgToCN_delete(String DestinationNodeID, String key)
    {
        try
        {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(hashTOport.get(DestinationNodeID)));
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader r= new BufferedReader(new InputStreamReader(socket.getInputStream()));
            w.write("DelVal*" + key + "\n");
            w.flush();
            String val=r.readLine();
            Log.i(TAG, "Deletion was done?" + val);
            socket.close();
            if (val.equals("Yes"))
                return true;
            else
                return false;
        }
        catch (Exception e)
        {
            Log.e(TAG, "Problem sending msg to CorrectNode!");
        }

        return false;

    }
    public String deleteHere(String fname)
    {
        File file = new File(getContext().getApplicationContext().getFilesDir().getPath() + "/" + fname);
        boolean deleted = file.delete();
        if (deleted)
        {
            Log.i(TAG, "deleted " + fname + "!");
            return "Yes";
        }
        else
        {
            Log.e(TAG, "Error in deleting- deleteHere!");
            return "No";
        }
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


    private class ServerTask extends AsyncTask<ServerSocket, String, Void>
    {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try
            {
                while (true) {
                    Socket s = serverSocket.accept();
                    //s.setSoTimeout(500);
                    BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    String inp=r.readLine();
                    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
                    if(inp.substring(0,8).equals("CheckKey"))
                    {
                        String reply=findNode(inp.substring(9));
                        w.write(reply+"\n");
                        w.flush();

                    }
                    else if(inp.equals("LastNode?"))
                    {
                        String reply="No";
                        if(islast)
                            reply="Yes";
                        w.write(reply +"\n");
                        w.flush();
                    }
                    else if(inp.substring(0,7).equals("NewData"))
                    {
                        String[] KeyVal=inp.substring(8).split(Pattern.quote(":"));
                        w.write("MsgRecvd"+"\n");
                        w.flush();
                        Integer retval=insertHere(KeyVal[0],KeyVal[1]);
                        if(retval==1)
                            Log.i(TAG, "Data was recvd and inserted");
                    }
                    else if(inp.substring(0,6).equals("GetVal"))
                    {
                        String value=queryHere(inp.substring(7));
                        w.write(value+"\n");
                        w.flush();
                    }
                    else if(inp.substring(0,6).equals("DelVal"))
                    {
                        String value=deleteHere(inp.substring(7));
                        w.write(value+"\n");
                        w.flush();
                    }
                    else if(inp.substring(0, 4).equals("JOIN")) // will return P&S
                    {
                        String[] joined_node=inp.substring(5).split(Pattern.quote("*"));
                        if(joined_nodes==0) {
                            w.write(REMOTE_PORT0 + "*" + REMOTE_PORT0 + "\n");
                            w.flush();
                            joined_nodes++;
                            ActiveNodes.put(joined_node[0], true);
                            predecessor=joined_node[1];  //setting P&S of 5554
                            successor=joined_node[1];
                            System.out.println(joined_node[0]+" was the joining node's ID. It's port is "+joined_node[1]);
                            Log.i(TAG, predecessor+" "+successor+" are now p&S @5554");
                            if(portTOhash.get(successor).compareTo(nodeID)<0)
                            {
                                islast = true;
                                Log.i(TAG, "This is the last node currently");
                            }
                            if(portTOhash.get(predecessor).compareTo(nodeID)>0)
                            {
                                isfirst = true;
                                Log.i(TAG, "This is the first node currently");
                            }
                        }
                        else
                        {
                            int flag=0;
                            ArrayList currentlyAN = new ArrayList();
                            for(Map.Entry<String,Boolean> entry : ActiveNodes.entrySet())
                            {
                                if (entry.getValue())
                                    currentlyAN.add(entry.getKey());
                            }
                            for (int i = 0; i < currentlyAN.size(); i++)
                            {
                                flag=i;
                                if(joined_node[0].compareTo(currentlyAN.get(i).toString())<0) //implies joined_node's ID < this key
                                {
                                    flag=-1;
                                    String np;
                                    if((i-1)>=0)
                                        np=hashTOport.get(currentlyAN.get(i - 1).toString());
                                    else
                                        np=hashTOport.get(currentlyAN.get(currentlyAN.size()-1).toString());
                                    String ns=hashTOport.get(currentlyAN.get(i).toString());
                                    w.write( np+ "*" + ns + "\n");
                                    w.flush();
                                    joined_nodes++;
                                    ActiveNodes.put(joined_node[0], true);
                                    System.out.println(joined_node[0] + " was the joining node's ID. It's port is " + joined_node[1]);

                                    //send new P&S to relevant Nodes
                                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ns));
                                    BufferedWriter wn1 = new BufferedWriter(new OutputStreamWriter(socket1.getOutputStream()));
                                    wn1.write("NEW_PRE*"+ hashTOport.get(joined_node[0]) + "\n");
                                    wn1.flush();
                                    Log.i(TAG, "Sent NewPre to "+ns);
                                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(np));
                                    BufferedWriter wn2 = new BufferedWriter(new OutputStreamWriter(socket2.getOutputStream()));
                                    wn2.write("NEW_SUC*" + hashTOport.get(joined_node[0]) + "\n");
                                    wn2.flush();
                                    Log.i(TAG, "Sent NewSuc to " + np);

                                    //Thread.sleep(4000);
                                    socket1.close();
                                    socket2.close();

                                    break;
                                }

                            }
                            if(flag!=-1)
                            {
                                //means the joining node has the highest ID yet. So it's successor will be the activenode with lowest ID
                                String np=hashTOport.get(currentlyAN.get(flag).toString());
                                String ns=hashTOport.get(currentlyAN.get(0).toString());
                                w.write( np+ "*" + ns + "\n");
                                w.flush();
                                joined_nodes++;
                                ActiveNodes.put(joined_node[0], true);
                                System.out.println(joined_node[0] + " was the joining node's ID. It's port is " + joined_node[1]);

                                //send new P&S to relevant Nodes
                                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ns));
                                BufferedWriter wn1 = new BufferedWriter(new OutputStreamWriter(socket1.getOutputStream()));
                                wn1.write("NEW_PRE*"+ hashTOport.get(joined_node[0]) + "\n");
                                wn1.flush();
                                Log.i(TAG, "Sent NewPre to "+ns);
                                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(np));
                                BufferedWriter wn2 = new BufferedWriter(new OutputStreamWriter(socket2.getOutputStream()));
                                wn2.write("NEW_SUC*" + hashTOport.get(joined_node[0]) + "\n");
                                wn2.flush();
                                Log.i(TAG, "Sent NewSuc to " + np);

                                //Thread.sleep(4000);
                                socket1.close();
                                socket2.close();

                            }

                        }

                    }
                    else if(inp.equals("GetSuccessor"))
                    {
                        w.write(successor + "\n");
                        w.flush();
                    }
                    else if(inp.equals("GetAllVal"))
                    {
                        Log.i(TAG,"Rcvd req for all local data!");
                        HashMap<String,String> KVpairs=AllLocalData("Query");
                        ObjectOutputStream o= new ObjectOutputStream(s.getOutputStream());
                        o.writeObject(KVpairs);
                        o.flush();
                        String ack=r.readLine();
                        if(ack.equals("Ack"))
                            Log.i(TAG,"Transmission of data successful!");
                        else
                            Log.e(TAG,"Data not recvd by other AVD!");


                    }else if(inp.equals("DelAllVal"))
                    {
                        Log.i(TAG, "Rcvd req for deleting all local data!");
                        HashMap<String,String> KVpairs=AllLocalData("Delete");
                        if(KVpairs.get("Deleted").equals("All"))
                        {
                            w.write("Yes"+"\n");
                            w.flush();
                            Log.i(TAG, "Deletion of local data successful!");
                        }
                        else
                        {
                            w.write("No"+"\n");
                            w.flush();
                            Log.i(TAG, "Deletion of local data NOT successful!");
                        }


                    }else if(inp.substring(0,7).equals("NEW_SUC"))
                    {
                        successor=inp.substring(8);
                        if(islast && portTOhash.get(successor).compareTo(nodeID)>0)
                        {
                            islast = false;
                            Log.i(TAG, "This is no more the last node");
                        }
                        Log.i(TAG, "NewSuc set to " + successor);
                    }
                    else if(inp.substring(0,7).equals("NEW_PRE"))
                    {
                        predecessor=inp.substring(8);
                        if(isfirst && portTOhash.get(predecessor).compareTo(nodeID)<0)
                        {
                            isfirst = false;
                            Log.i(TAG, "This is no more the first node");
                        }
                        Log.i(TAG, "NewPre set to " + predecessor);
                    }

                    s.close();
                }

            }
            catch (IOException e) {
                Log.e(TAG, "socket/DiB issue"+e);
            }

            return null;
        }


    }



    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {

            try {
                String remotePort = REMOTE_PORT0;  //connecting to 5554
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                BufferedWriter w = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                BufferedReader r = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                socket.setSoTimeout(700);
                w.write(msgs[0] + "*" + msgs[1] + "\n");
                w.flush();

                String Rep54=r.readLine();
                if(Rep54==null)
                {
                    Log.i(TAG,"Null reply. Looks like 5554 is not active!");
                    throw new SocketTimeoutException();
                }
                String[] pns= Rep54.split(Pattern.quote("*"));
                predecessor=pns[0];
                successor=pns[1];
                if(portTOhash.get(successor).compareTo(nodeID)<0)
                {
                    islast = true;
                    Log.i(TAG, "This is the last node currently");
                }
                if(portTOhash.get(predecessor).compareTo(nodeID)>0)
                {
                    isfirst = true;
                    Log.i(TAG, "This is the first node currently");
                }
                Log.i(TAG, predecessor+" "+successor+" are now p&S @"+DevId);
            }
            catch (SocketTimeoutException e){
                Log.i(TAG, "Socket Timeout at CT.");
                predecessor=hashTOport.get(nodeID);
                successor=predecessor;
                Log.i(TAG, "P&S set to node itself");
            }catch(UnknownHostException e){
                Log.e(TAG, "ClientTask UnknownHostException" + e);
            }catch(IOException e){
                Log.e(TAG, "ClientTask socket IOException" + e);
            }


            return null;
        }
    }


}
