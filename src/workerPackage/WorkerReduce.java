package workerPackage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.stream.Collectors;

import tests.WordCount;

public class WorkerReduce {
	String configFile = "";
	public void initialize(String configFile, Object objRef, int numFilesToRead)
	{
		System.out.println("reached reducer");
		//_reduceFunction = customReduce;
		this.configFile = configFile;
		_numFilesToRead = numFilesToRead;
	}

	//Function to print key value pair from intermediate file
	public static void printProperties(Properties prop)
    {	
        for (Object key: prop.keySet()) {
           // System.out.println(key + ": " + prop.getProperty(key.toString()));
        }
    }

	// Emit Final function to store the final key value pair
	public static void emitFinal(String key, Integer value){
		reduceProp.put(key,value);
	}
	
	public void perform()
	{
		System.out.println("Worker Reduce called");
		Properties prop = new Properties();
		try (FileInputStream fis = new FileInputStream(this.configFile)) {
			prop.load(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Properties mapProperties[] = new Properties[_numFilesToRead];

		String dirName = System.getProperty("user.dir") + "/"+_reducerId;
		
		String intermediateFile = prop.getProperty("intermediate");
		
		for (int i =0; i<_numFilesToRead; i++)
		{
			String newIntermediateFileName = dirName +"/"+ i + intermediateFile;
			System.out.println(newIntermediateFileName);	
			//for eclipse debugging
			//intermediateFile = "src/"+intermediateFile;
			//System.out.println(intermediateFile);

			
			try (FileInputStream fis = new FileInputStream(newIntermediateFileName)) {
				mapProperties[i] = new Properties();
				mapProperties[i].load(fis);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			mapProp.putAll(mapProperties[i]);
			
			File correspondingFile = new File(newIntermediateFileName);
			correspondingFile.delete();
			
			if (i == _numFilesToRead-1){
				File correspondingDir = new File(dirName);
				correspondingDir.delete();
			}
		}
		
		Class[] emitFinalArgs = {String.class, Integer.class};
		try {
			for (Object key: mapProp.keySet()) {
				//System.out.println(key + ": " + mapProp.getProperty(key.toString()));
				List<String> values = Arrays.stream(mapProp.getProperty(key.toString()).split(",")).collect(Collectors.toList());
				//System.out.println(values);
				_reduceFunction.invoke(_callerObject, String.valueOf(key), values, this.getClass().getDeclaredMethod("emitFinal", emitFinalArgs), this);
			}

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}

		System.out.println("done");
	}
	
	public static void main(String[] args) {
		System.out.println("From Reducer-main");
		boolean isConnected = false;
		_reducerId = Integer.parseInt(args[0]);
		int choice = Integer.parseInt(args[3]);
		
		while (!isConnected) {
			
			int port = 4459+_reducerId;
			System.out.println("In loop to wait for "+port);
			try {
				ServerSocket serverSocket = new ServerSocket(port);
				Socket socket = serverSocket.accept();
				System.out.println("Connected");
				isConnected = true;
				ObjectInputStream inStream = new ObjectInputStream(socket.getInputStream());

				_callerObject = (Object) inStream.readObject();
				
				Class[] reduceArgs = {String.class, List.class, Method.class, WorkerReduce.class};
				try {
					_reduceFunction = (Method) _callerObject.getClass().getDeclaredMethod("reduce", reduceArgs);
					_reduceFunction.setAccessible(true);
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
		int numFilesToRead = Integer.parseInt(args[2]);
		String fileName = args[1];
		WorkerReduce wr1 = new WorkerReduce();
		wr1.initialize(fileName, _callerObject, numFilesToRead);
		wr1.perform();
		return;
	}
	
	static Method _reduceFunction;
	static int _reducerId;
	int _numFilesToRead;
	static Object _callerObject;
	public Properties mapProp = new Properties();
	public static Hashtable<String, Integer> reduceProp = new Hashtable<String, Integer>();
}
