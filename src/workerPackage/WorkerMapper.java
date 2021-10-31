package workerPackage;

import java.lang.reflect.InvocationTargetException;
import utilsPackage.GenericStreamObjectWrapper;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;

import tests.WordCount;

import java.io.*;
import utilsPackage.TestClass;

//This is our Mapper Class
public class WorkerMapper {
	
	public void initialize(String configFile, Object objRef, int numFilesToEmit)
	{
		System.out.println("reached mapper");
		//_mapFunction = customMap;
		System.out.println(configFile);

		Properties prop = new Properties();
		String fileName = configFile;
		try (FileInputStream fis = new FileInputStream(fileName)) {
			prop.load(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//numMappers = prop.getProperty("mappers");
		//numReducers = prop.getProperty("reducers");
		inputFile = prop.getProperty("input");
		intermediateFile = prop.getProperty("intermediate");
		//_startByteToRead = start;
		//_endByteToRead = end;
		_numFilesToEmit = numFilesToEmit;
	}
	//Emit intermediate function, which saves intermediate key value pairs to a Properties variable mapProp
	public void emitIntermediate(String key, String value){
		
		if(mapProp.getProperty(key) == null)
		{
			mapProp.put(key,value);
		}
		else{
			String new_value = mapProp.getProperty(key).concat(",");
			new_value = new_value.concat(value);
			mapProp.put(key,new_value);
		}
	}
	public void perform()
	{
		System.out.println("Worker Mapper called");
		try {
			//System.out.println(inputFile);
			
			// for eclipse debugging
			//inputFile = "src/"+inputFile;
		
			/*For error logging
			 * try{
				File myObj = new File("filename.txt");
				myObj.createNewFile();
				try {
					FileWriter myWriter = new FileWriter("filename.txt");
				    BufferedWriter bw = new BufferedWriter(myWriter);
				    bw.write("input is "+inputFile);
				    bw.newLine();
				    bw.close();
					myWriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}			
			} catch (IOException e) {
				e.printStackTrace();
			}*/
			
			File file = new File(inputFile);
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(file));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			
			String st;
			int Counter = 0;
			Class[] emitIntermediateArgs  = {String.class, String.class};
			try {
				while ((st = br.readLine()) != null){
					try {
						WorkerMapper._mapFunction.invoke(_callerObject, String.valueOf(Counter), st, this.getClass().getDeclaredMethod("emitIntermediate", emitIntermediateArgs),intermediateFile, _numFilesToEmit, _mapperId, this);
					} catch (NoSuchMethodException e) {
						e.printStackTrace();
					} catch (SecurityException e) {
						e.printStackTrace();
					}
	   			}		
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			

		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		System.out.println("From Mapper-main"+args.length);
		boolean isConnected = false;
		_mapperId = Integer.parseInt(args[0]);
		int choice = Integer.parseInt(args[3]);
		
		while (!isConnected) {
			
			int port = 4459+_mapperId;
			System.out.println("In loop to wait for "+port);
			try {
				ServerSocket serverSocket = new ServerSocket(port);
				Socket socket = serverSocket.accept();
				System.out.println("Connected");
				isConnected = true;
				ObjectInputStream inStream = new ObjectInputStream(socket.getInputStream());

				_callerObject = (Object) inStream.readObject();
				
				Class[] mapArgs = {String.class, String.class, Method.class, String.class, Integer.class, Integer.class, WorkerMapper.class};
				try {
					_mapFunction = (Method) _callerObject.getClass().getDeclaredMethod("map", mapArgs);
					_mapFunction.setAccessible(true);
				} catch (NoSuchMethodException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SecurityException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//message.boom();
				System.out.println("Object received");
				socket.close();
				serverSocket.close();

			} 
			catch (SocketException se) {
				System.out.println("Exception1");
				se.printStackTrace();
				System.exit(0);
			} catch (IOException e) {
				System.out.println("Exception2");
				e.printStackTrace();
			} catch (ClassNotFoundException cn) {
				System.out.println("Exception3");
				cn.printStackTrace();
			}
		}
		int numFilesToEmit = Integer.parseInt(args[2]);
		String fileName = args[1];
		WorkerMapper wm1 = new WorkerMapper();
		wm1.initialize(fileName, _callerObject, numFilesToEmit);
		wm1.perform();
		return;
	}
	
	public static int check;
	static Method _mapFunction;
	static int _mapperId;
	int _startByteToRead, _endByteToRead;
	int _numFilesToEmit;
	String numMappers;
	String numReducers;
	String inputFile;
	String intermediateFile;
	static Object _callerObject;
	public Properties mapProp = new Properties();
	
}
