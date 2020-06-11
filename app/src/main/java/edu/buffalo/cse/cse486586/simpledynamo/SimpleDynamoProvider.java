package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import android.annotation.SuppressLint;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {
	Context context;
	String myhash;
	int prevNode = 0;
	int nextNode = 0;
	int nextNextNode = 0;
	String nexthash;
	String prevhash;
	private int joinPort = 5554;
	private int myPort = 0;
	static final int SERVER_PORT = 10000;
	Uri myUri = null;
	private volatile int queryInitiatorPort = 0;
	private ArrayList<String> joinedNodesList = new ArrayList<String>();
	private HashMap<String, Integer> hashPortMap = new HashMap<String, Integer>();
	private ArrayList<String> hashKeyList = new ArrayList<String>();
	private ArrayList<Integer> orderedPorts = new ArrayList<Integer>();
	private volatile boolean isReplica = false;
	private final Semaphore threadAccess = new Semaphore(1, true);
	private boolean localquery = false;


	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String filename = selection;
		String hashedKey = null;
		if(context == null) {
			context = getContext();
		}

		try {
			hashedKey = genHash(filename);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		int targetNode = getNodeForKey(filename);

		if(filename.equals("@")) {
			// return all local key values
			String[] listKeys = context.fileList();
			try {
				for (String key : listKeys) {
					File file = context.getFileStreamPath(key);
					boolean isdeleted = file.delete();
					Log.i(TAG, "file deleted? " + isdeleted);
					String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicatedDelete#" + filename + "#1", nextNode*2).get();
					if(resp.equals("failed")) {
						resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicatedDelete#" + filename + "#2", nextNextNode*2).get();
					}
				}
			} catch (Exception e) {
				Log.e(TAG, "Error reading all local files " + e);
			}
		} else if(filename.equals("*")) {
			String resp = null;
			String[] listKeys = context.fileList();
			for (String key : listKeys) {
				File file = context.getFileStreamPath(key);
				boolean isdeleted = file.delete();
				Log.i(TAG, "file deleted? " + isdeleted);
			}
			try {
				resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "delete#*#"+queryInitiatorPort, nextNode*2).get();
				if(resp.equals("failed")) {
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "delete#*#"+queryInitiatorPort, nextNextNode*2).get();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

		} else if (isReplica || targetNode == myPort) {
			try {
				File file = context.getFileStreamPath(filename);
				boolean isdeleted = file.delete();
				if(!isReplica) {
					String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicatedDelete#" + filename + "#1", nextNode*2).get();
					if(resp.equals("failed")) {
						resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicatedDelete#" + filename + "#2", nextNextNode*2).get();
					}
				}
				isReplica = false;
				Log.i(TAG, "Stored in cursor and returned");
			} catch (Exception e) {
				Log.e(TAG, "File read failed");
			}
		} else {
			try {
				String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "delete#" + filename + "#"+queryInitiatorPort, targetNode*2).get();
				int nextTargetPort = 0;
				if(resp.equals("failed")) {
					int targetIndex = orderedPorts.indexOf(targetNode);
					if(targetIndex == orderedPorts.size()-1) {
						nextTargetPort = orderedPorts.get(0);
					} else {
						nextTargetPort = orderedPorts.get(targetIndex+1);
					}
					resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicatedDelete#" + filename + "#1", nextTargetPort*2).get();
					Log.i(TAG, "next target response: " + resp);
				}
				if(resp.equals("failed")) {
					int targetIndex = orderedPorts.indexOf(nextTargetPort);
					int nextTargetPort2 = 0;
					if(targetIndex == orderedPorts.size()-1) {
						nextTargetPort2 = orderedPorts.get(0);
					} else {
						nextTargetPort2 = orderedPorts.get(targetIndex+1);
					}
					resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicatedDelete#" + filename + "#2", nextTargetPort2*2).get();
					Log.i(TAG, "next next target response: " + resp);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
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

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		Log.i(TAG, "INSERT: inside insert method");
		synchronized (values) {
			String filename = (String) values.get("key");
			String string = (String) values.get("value");
			if(context == null) {
				context = getContext();
			}

			int targetNode = getNodeForKey(filename);
			Log.i(TAG, "INSERT: variables initialized");
			Log.i(TAG, "INSERT: key " + filename + " value " + string + " to " + targetNode);
			try {
				if(targetNode == myPort) {
					localInsert(filename, string, 0);
					return uri;
				}
				String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "insert#" + filename + "#" + string, targetNode * 2).get();
				Log.i(TAG, "Insert target resp: " + resp);
				int nextTargetPort = 0;
				if(resp.equals("failed")) {
					int targetIndex = orderedPorts.indexOf(targetNode);
					if(targetIndex == orderedPorts.size()-1) {
						nextTargetPort = orderedPorts.get(0);
					} else {
						nextTargetPort = orderedPorts.get(targetIndex+1);
					}
					if(nextTargetPort == myPort) {
						localInsert(filename, string, 1);
						return uri;
					}
					resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicate#"+filename+"#"+string+"#1", nextTargetPort*2).get();
					Log.i(TAG, "Insert next target resp: " + resp);
				}
				if(resp.equals("failed")) {
					int targetIndex = orderedPorts.indexOf(nextTargetPort);
					int nextTargetPort2 = 0;
					if(targetIndex == orderedPorts.size()-1) {
						nextTargetPort2 = orderedPorts.get(0);
					} else {
						nextTargetPort2 = orderedPorts.get(targetIndex+1);
					}
					if(nextTargetPort2 == myPort) {
						localInsert(filename, string, 2);
						return uri;
					}
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicate#" + filename + "#" + string + "#2", nextTargetPort2 * 2).get();
					Log.i(TAG, "next next target response: " + resp);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		/*if(isReplica || targetNode == myPort) {
			FileOutputStream outputStream;
			Log.i(TAG, "INSERT: need to insert locally " + myPort);

			try {
				//synchronized(this) {
					try {
						Log.i(TAG, "INSERT: semaphore acquired");
						outputStream = context.openFileOutput(filename, Context.MODE_PRIVATE);
						outputStream.write(string.getBytes());
						outputStream.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				//}
				Log.i(TAG, "INSERT: semaphore released");
				Log.i(TAG, "isReplica : " + isReplica );
				if(!isReplica) {
					Log.i(TAG, "Forwarding replica to " + nextNode + " and " + nextNextNode + " " + filename);
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicate#"+filename+"#"+string, nextNode*2);
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicate#"+filename+"#"+string, nextNextNode*2);
				}
				isReplica = false;
			} catch (Exception e) {
				Log.e(TAG, "File write failed");
				e.printStackTrace();
			}
		} else {
			// forward
			Log.i(TAG, "INSERT: forwarded to " + targetNode + " query: " + filename);
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "insert#"+filename+"#"+string, targetNode*2);

		}*/

		Log.v("insert", values.toString());
		return uri;
	}

	public int getNodeForKey(String key) {
		if(key.equals("*") || key.equals("@")) {
			return myPort;
		}
		int node = 0;
		String hashedKey = null;
		try {
			hashedKey = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		if(hashedKey.compareTo(hashKeyList.get(0)) < 0 || hashedKey.compareTo(hashKeyList.get(hashKeyList.size()-1)) >= 0) {
			node = hashPortMap.get(hashKeyList.get(0));
		} else {
			String prevNodehash = hashKeyList.get(0);
			for(String nodehash: hashKeyList) {
				if(hashedKey.compareTo(nodehash) < 0 && hashedKey.compareTo(prevNodehash) >= 0) {
					node = hashPortMap.get(nodehash);
					break;
				}
			}
		}
		return node;
	}

	public synchronized String localInsert(String key, String value, int replicaCnt) {
		try {
			threadAccess.acquire();
			FileOutputStream outputStream = null;
			outputStream = context.openFileOutput(key, Context.MODE_PRIVATE);
			outputStream.write(value.getBytes());
			outputStream.close();
			String resp =  "OK";

			if(replicaCnt == 0 || replicaCnt == 1) {
				Log.i(TAG, "message being sent for replica: " + "replicate#"+key+"#"+value+"#" + (replicaCnt+1));
				resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicate#"+key+"#"+value+"#" + (replicaCnt+1), nextNode*2).get();
				if(resp.equals("failed") && replicaCnt == 0) {
					resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicate#"+key+"#"+value+"#" + (replicaCnt+2), nextNextNode*2).get();
				}
			}
			return resp;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} finally {
			threadAccess.release();
		}
		return "failed";
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		if(context == null) {
			context = getContext();
		}

		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
		uriBuilder.scheme("content");
		myUri = uriBuilder.build();

		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = Integer.parseInt(portStr);
		queryInitiatorPort = myPort;
		try {
			myhash = genHash(Integer.toString(myPort));

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			e.printStackTrace();
		}

		int i = 5554;
		while (i < 5564) {
			try {
				String hashKey = genHash(Integer.toString(i));
				hashPortMap.put(hashKey, i);
				hashKeyList.add(hashKey);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			i += 2;
		}
		Collections.sort(hashKeyList);
		for(String hashkey: hashKeyList) {
			orderedPorts.add(hashPortMap.get(hashkey));
		}
		int myhashindex = hashKeyList.indexOf(myhash);
		if(myhashindex == 0) {
			prevhash = hashKeyList.get(hashKeyList.size()-1);
			prevNode = hashPortMap.get(prevhash);
		} else {
			prevhash = hashKeyList.get(myhashindex-1);
			prevNode = hashPortMap.get(prevhash);
		}
		if(myhashindex == hashKeyList.size()-1) {
			nexthash = hashKeyList.get(0);
			nextNode = hashPortMap.get(nexthash);
			nextNextNode = hashPortMap.get(hashKeyList.get(1));
		} else {
			nexthash = hashKeyList.get(myhashindex+1);
			nextNode = hashPortMap.get(nexthash);
			if(myhashindex+1 == hashKeyList.size()-1) {
				nextNextNode = hashPortMap.get(hashKeyList.get(0));
			} else {
				nextNextNode = hashPortMap.get(hashKeyList.get(myhashindex+2));
			}
		}

		try {
			String resp = new RecoverNodeTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "").get();
			if(resp.equals("failed")) {
				Thread.sleep(2000);
				new RecoverNodeTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
									 String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.i(TAG, "Query: inside query method");
		/*try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/
		String filename = selection;
		FileInputStream inputStream = null;
		MatrixCursor cursor = new MatrixCursor(
				new String[] {"key", "value"}, 5
		);
		if(context == null) {
			context = getContext();
		}
		int targetNode = getNodeForKey(filename);
		Log.i(TAG, "Query: variables initialized");
		Log.i(TAG, "Query: query: " + selection);
		Log.i(TAG, "Query: target node, myPort: " + targetNode + ", " + myPort);

		if(filename.equals("@")) {
			// return all local key values
			String[] listKeys = context.fileList();
			try {
				InputStreamReader inputStreamReader = null;
				BufferedReader bufferedReader = null;
				for (String key : listKeys) {
					inputStream = context.openFileInput(key);
					inputStreamReader = new InputStreamReader(inputStream);
					bufferedReader = new BufferedReader(inputStreamReader);
					String line = bufferedReader.readLine();
					cursor.addRow(new String[]{key, line});
				}
				inputStreamReader.close();
				inputStream.close();
				bufferedReader.close();

			} catch (Exception e) {
				Log.e(TAG, "Error reading all local files " + e);
			}
		} else if(filename.equals("*")) {
			try {
				String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "query1#*#"+queryInitiatorPort, nextNode*2).get();
				cursor = (MatrixCursor) query(myUri, null, "@", null, null);
				if(resp.equals("failed")) {
					resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "query1#*#"+queryInitiatorPort, nextNextNode*2).get();
				}
				Log.i(TAG, "Query: remote data obtained " + resp);
				String[] clientKeyvalues = {};
				if(resp != "" && resp != null) {
					clientKeyvalues = resp.split(",");
				}
				for(String keyval: clientKeyvalues) {
					if(keyval.isEmpty()) {
						continue;
					}
					String[] keyvalueArr = keyval.split("#");
					cursor.addRow(keyvalueArr);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

		} else if (targetNode == myPort || localquery) {
			Log.i(TAG, "Query: this is a local query " + localquery);
			try {
				localquery = false;
				inputStream = context.openFileInput(filename);
				Log.i(TAG, "Query: file obtained ");
				InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
				StringBuilder sb = new StringBuilder();
				String line = bufferedReader.readLine();
				inputStream.close();
				inputStreamReader.close();
				bufferedReader.close();
				Log.i(TAG, "Query: value read: " + line);
				sb.append(line);
				cursor.addRow(new String[]{filename, line});
				Log.i(TAG, "Query: value added to the cursor");

			} catch (Exception e) {
				Log.e(TAG, "File read failed");
				e.printStackTrace();
			}
		} else {
			Log.i(TAG, "Query: needs to be forwarded to " + targetNode);
			try {
				String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "query1#" + filename + "#" + queryInitiatorPort, targetNode*2).get();
				Log.i(TAG, "Query: response from " + targetNode + " resp " + resp);
				int nextTargetPort = 0;
				if(resp.equals("failed") || resp.isEmpty()) {
					int targetIndex = orderedPorts.indexOf(targetNode);
					if(targetIndex == orderedPorts.size()-1) {
						nextTargetPort = orderedPorts.get(0);
					} else {
						nextTargetPort = orderedPorts.get(targetIndex+1);
					}
					resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "querylocal#" + filename + "#" + queryInitiatorPort, nextTargetPort*2).get();
					Log.i(TAG, "next target response: " + resp);
				}
				if(resp.equals("failed") || resp.isEmpty()) {
					int targetIndex = orderedPorts.indexOf(nextTargetPort);
					int nextTargetPort2 = 0;
					if(targetIndex == orderedPorts.size()-1) {
						nextTargetPort2 = orderedPorts.get(0);
					} else {
						nextTargetPort2 = orderedPorts.get(targetIndex+1);
					}
					resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "querylocal#" + filename + "#" + queryInitiatorPort, nextTargetPort2*2).get();
					Log.i(TAG, "next next target response: " + resp);
				}
				String[] clientKeyvalues = {};
				if(resp != "" && resp != null) {
					clientKeyvalues  = resp.split(",");
				}
				Log.i(TAG, "Query: resp in array: " + clientKeyvalues);
				for(String keyval: clientKeyvalues) {
					if(keyval.isEmpty()) {
						continue;
					}
					String[] keyvalueArr = keyval.split("#");
					cursor.addRow(keyvalueArr);
				}
				Log.i(TAG, "Query: response added to the cursor");
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return cursor;
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

	private class ClientTask extends AsyncTask<Object, Void, String> {

		@Override
		protected String doInBackground(Object... params) {
			String msg = (String) params[0];
			int remotePort = (Integer) params[1];
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						remotePort);
				socket.setSoTimeout(12000);
				DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
				dataOutputStream.writeUTF(msg);
				dataOutputStream.flush();
				DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
				String resp = dataInputStream.readUTF();
				socket.close();
				//dataInputStream.close();
				//dataOutputStream.close();
				Log.i(TAG, "Client Task: everything is closed. returning resp");
				return resp;
			} catch (SocketTimeoutException e) {
				e.printStackTrace();
				return "failed";
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
				e.printStackTrace();
			}

			return "failed";
		}
	}

	private class RecoverNodeTask extends AsyncTask<Object, Void, String> {

		@Override
		protected String doInBackground(Object... params) {
			try {
				SharedPreferences preferences = context.getSharedPreferences("DynamoPref", Context.MODE_PRIVATE);
				Log.i(TAG, "shared pref: " + preferences.contains("FirstRun"));
				if (!preferences.contains("FirstRun")) {
					SharedPreferences.Editor editor = preferences.edit();
					editor.putInt("FirstRun", 0);
					editor.commit();
					Log.i(TAG, "returning ok");
					return "OK";
				}
				String allKVs = "";
				Socket nextSocket = null;
				Socket prevSocket = null;
				DataOutputStream dataOutputStream = null;
				DataInputStream dataInputStream = null;
				try {
					nextSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							nextNode * 2);
					nextSocket.setSoTimeout(5000);
					dataOutputStream = new DataOutputStream(nextSocket.getOutputStream());
					dataOutputStream.writeUTF("recovery#" + myPort);
					dataOutputStream.flush();
					dataInputStream = new DataInputStream((nextSocket.getInputStream()));
					allKVs += dataInputStream.readUTF();
					dataOutputStream.close();
					dataInputStream.close();
				} catch (SocketTimeoutException e) {
					e.printStackTrace();
				}
				nextSocket.close();

				Log.i(TAG, "RecoveryNode from " + nextNode + " : " + allKVs);

				try {
					prevSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							prevNode * 2);
					prevSocket.setSoTimeout(5000);
					dataOutputStream = new DataOutputStream(prevSocket.getOutputStream());
					dataOutputStream.writeUTF("recovery#" + myPort);
					dataOutputStream.flush();
					dataInputStream = new DataInputStream((prevSocket.getInputStream()));
					allKVs += dataInputStream.readUTF();
					dataOutputStream.close();
					dataInputStream.close();
				} catch (SocketTimeoutException e) {
					e.printStackTrace();
				}
				prevSocket.close();

				String[] kvArr = allKVs.split(",");
				Log.i(TAG, "RecoveryNode from " + prevNode + " : " + allKVs);
				FileOutputStream outputStream = null;
				for(String kv: kvArr) {
					String[] keyval = kv.split("#");
					outputStream = context.openFileOutput(keyval[0], Context.MODE_PRIVATE);
					outputStream.write(keyval[1].getBytes());
				}
				outputStream.close();

				Log.i(TAG, "Recover Task: everything is closed. returning resp");
				return "OK";
			} catch (UnknownHostException e) {
				Log.e(TAG, "Recover Task UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "Recover Task socket IOException");
				e.printStackTrace();
			}

			return "failed";
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			String line = "";
			if(context == null) {
				context = getContext();
			}
			try {
				while(true) {
					Socket socket = serverSocket.accept();
					DataInputStream inputMsg = new DataInputStream(socket.getInputStream());
					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					String msg = "";
					if(inputMsg != null)
						msg = inputMsg.readUTF();
					Log.i(TAG, "Server Task: " + msg);
					if(msg.contains("query1") || msg.contains("querylocal")) {
						//threadAccess.acquire();
						String query = msg.split("#")[1];
						Log.i(TAG, "forwarded query received " + query);
						queryInitiatorPort = new Integer(msg.split("#")[2]);
						Log.i(TAG, "forwarded query: queryInitiatorPort " + queryInitiatorPort);
						String allKVs = "";
						Log.i(TAG, "forwarded query: myport " + myPort);

						if(!(query.equals("*") && queryInitiatorPort == myPort)) {
							if(msg.contains("querylocal")) {
								Log.i(TAG, "This is a querylocal");
								localquery = true;
							}
							Cursor qResult = query(myUri, null, query, null, null);
							// To do: process cursor and respond with arraylist
							qResult.moveToFirst();

							while (!qResult.isAfterLast()) {
								String key = qResult.getString(qResult.getColumnIndex("key"));
								String value = qResult.getString(qResult.getColumnIndex("value"));
								allKVs += key+ "#" + value + ",";
								qResult.moveToNext();
							}
							//qResult.close();
						}
						queryInitiatorPort = myPort;

						dataOutputStream.writeUTF(allKVs);
						dataOutputStream.flush();
						Log.i(TAG, "query results: " + allKVs);
						//threadAccess.release();
						socket.close();
					} else if(msg.contains("recovery")) {
						int recoveryPort = new Integer(msg.split("#")[1]);
						try {
							int recoveryInd = orderedPorts.indexOf(recoveryPort);
							int myInd = orderedPorts.indexOf(myPort);
							String recoveryHash = hashKeyList.get(recoveryInd);
							Log.i(TAG, "recovery: orderedPorts: " + orderedPorts);
							int recoveryPrevNode = 0;
							String recoveryPrevHash;
							if(recoveryInd == 0) {
								recoveryPrevNode = orderedPorts.get(4);
								recoveryPrevHash = hashKeyList.get(4);
							} else {
								recoveryPrevNode = orderedPorts.get(recoveryInd-1);
								recoveryPrevHash = hashKeyList.get(recoveryInd-1);
							}
							String allKVs = "";
							if(recoveryPrevNode == myPort) {
								Log.i(TAG, "Recovery: I am prev node: myport, recoveryPort: " + myPort + ", " + recoveryPort);
								Cursor qResult = query(myUri, null, "@", null, null);
								// To do: process cursor and respond with arraylist
								qResult.moveToFirst();
								String tempKVs = "";
								int prevprevNode = 0;
								String prevprevHash = null;
								int prevInd = orderedPorts.indexOf(prevNode);
								Log.i(TAG, "prev node: " + prevNode);
								if(prevInd == 0) {
									prevprevNode = orderedPorts.get(4);
									prevprevHash = hashKeyList.get(4);

								} else {
									prevprevNode = orderedPorts.get(prevInd-1);
									prevprevHash = hashKeyList.get(prevInd-1);
								}
								Log.i(TAG, "prev prev node: " + prevprevNode + " " + prevprevHash);

								while (!qResult.isAfterLast()) {
									String key = qResult.getString(qResult.getColumnIndex("key"));
									String value = qResult.getString(qResult.getColumnIndex("value"));
									String hashedKey = genHash(key);

									tempKVs += key+ "#" + value + ",";
									if((hashedKey.compareTo(prevprevHash) >= 0 && hashedKey.compareTo(myhash) < 0) || ((prevInd == 0 || prevInd == 4) && (hashedKey.compareTo(prevprevHash) >= 0 || hashedKey.compareTo(myhash) < 0))) {
										allKVs += key+ "#" + value + ",";
									}
									qResult.moveToNext();
								}
								qResult.close();
								dataOutputStream.writeUTF(allKVs);
								dataOutputStream.flush();
								Log.i(TAG, "Recovery: conditional kv: " + allKVs);
								Log.i(TAG, "Recovery: all kv: " + tempKVs);
							} else {
								Log.i(TAG, "Recovery: I am next node: myport, recoveryPort: " + myPort + ", " + recoveryPort);
								Cursor qResult = query(myUri, null, "@", null, null);
								// To do: process cursor and respond with arraylist
								qResult.moveToFirst();
								String tempKVs = "";

								while (!qResult.isAfterLast()) {
									String key = qResult.getString(qResult.getColumnIndex("key"));
									String value = qResult.getString(qResult.getColumnIndex("value"));
									String hashedKey = genHash(key);

									tempKVs += key+ "#" + value + ",";
									if((hashedKey.compareTo(recoveryPrevHash) >= 0 && hashedKey.compareTo(recoveryHash) < 0) || ((recoveryInd == 0) && (hashedKey.compareTo(recoveryPrevHash) >= 0 || hashedKey.compareTo(recoveryHash) < 0))) {
										allKVs += key+ "#" + value + ",";
									}
									qResult.moveToNext();
								}
								qResult.close();
								Log.i(TAG, "Recovery all kvs: " + tempKVs);
								Log.i(TAG, "Recovery conditional kvs: " + allKVs);
								dataOutputStream.writeUTF(allKVs);
								dataOutputStream.flush();
							}
							Log.i(TAG, "query results: " + allKVs);
							socket.close();
						} catch (NoSuchAlgorithmException e) {
							e.printStackTrace();
						}
					} else if(msg.contains("insert")) {
						Log.i(TAG, "Server Task: insert request received");
						String key = msg.split("#")[1];
						String value = msg.split("#")[2];
						Log.i(TAG, "Server Task: insert key, value: " + key + ", " + value);
						Log.i(TAG, "Server Task: local insert method calling...");
						String resp = localInsert(key, value, 0);
						dataOutputStream.writeUTF(resp);
						dataOutputStream.flush();
						socket.close();
					} else {
						publishProgress(msg);
						dataOutputStream.writeUTF("OK");
						dataOutputStream.flush();
						socket.close();
					}
					//dataOutputStream.close();

					//inputMsg.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

			return null;
		}

		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}

		protected void onProgressUpdate(String...strings) {
			Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
			ContentValues cv = new ContentValues();

			String strReceived = strings[0];
			if(strReceived.contains("delete")) {
				String query = strReceived.split("#")[1];
				queryInitiatorPort = new Integer(strReceived.split("#")[2]);
				if(myPort != queryInitiatorPort) {
					delete(myUri, query, null);
				}
			} else if(strReceived.contains("replicatedDelete")) {
				String key = strReceived.split("#")[1];
				int replicaCnt = new Integer(strReceived.split("#")[2]);
				isReplica = true;
				delete(myUri, key, null);
				if(replicaCnt == 1) {
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "replicatedDelete#"+key+"#2", nextNode*2);
				}
			} else if(strReceived.contains("replicate")) {
				String key = strReceived.split("#")[1];
				String value = strReceived.split("#")[2];
				int replicaCnt = new Integer(strReceived.split("#")[3]);
				localInsert(key, value, replicaCnt);
			}

			return;
		}
	}
}
