package controlPackage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.lang.Math;

import workerPackage.WorkerMapper;
import workerPackage.WorkerReduce;
import utilsPackage.GenericStreamObjectWrapper;
import utilsPackage.TestClass;

public class Master {

	public void initialize(Method customMap, Method customReduce, String configFile, Object objRef)
	{
		System.out.println("reached master");

		Properties prop = new Properties();
		_configFileName = configFile;
		try (FileInputStream fis = new FileInputStream(_configFileName)) {
			prop.load(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		_customMap = customMap;
		String inputFile = prop.getProperty("input");
		File f = new File(inputFile);
        double fileSize = f.length();
        
		_numMappers = Integer.parseInt(prop.getProperty("mappers"));
		_numReducers = Integer.parseInt(prop.getProperty("reducers"));

		double numBytesPerPartition = fileSize/_numMappers;
		
		_refObject = objRef;
		
		for (int i =0; i<_numMappers; i++)
		{
			//_mapperNodes[i] = new WorkerMapper();
			int roundedNumBytes =(int)Math.ceil(numBytesPerPartition); 
			int startByte = i*roundedNumBytes;
			int endByte = ((i+1)*roundedNumBytes)-1;
			//_mapperNodes[i].initialize(customMap,configFile, objRef, startByte, endByte, _numReducers);
		}

		for (int i =0; i<_numReducers; i++)
		{
			//_reducerNodes[i] = new WorkerReduce();	
			//_reducerNodes[i].initialize(customReduce, configFile, objRef, _numMappers);
		}
	}
	
	public void perform(Object mappingObj, Object reducerObject, int choice)
	{
		System.out.println("Calling from Master");
		String javaHome = System.getProperty("java.home");
	    String javaBin = javaHome + "/bin/java";
	    String classpath = System.getProperty("java.class.path");
	    String className = "workerPackage.WorkerMapper";
	    System.out.println(className);

		{
			Process[] process = new Process[_numMappers];
			for (int i = 0; i < _numMappers; i++) {
				try {
				    ArrayList<String> args = new ArrayList<String>();
					ArrayList<String> command = new ArrayList<String>();
					command.add(javaBin);
					int id = i;
					args.add(0, String.valueOf(id));
					args.add(1, _configFileName);
					args.add(2, String.valueOf(_numReducers));
					args.add(3, String.valueOf(choice));
					command.add("-classpath");
					command.add(classpath);
					command.add(className);
					command.addAll(args);

					ProcessBuilder builder = new ProcessBuilder(command);
					process[i] = builder.inheritIO().start();

					boolean isConnected = false;
					while (!isConnected) {
						try {
							int port = 4459 + i;
							Socket socket = new Socket("localHost", port);
							System.out.println("Connected master" + port);
							isConnected = true;

							OutputStream outputStream = socket.getOutputStream();
							ObjectOutputStream objStream = new ObjectOutputStream(outputStream);


							//GenericStreamObjectWrapper methodContainer = new GenericStreamObjectWrapper();
							//methodContainer.setMapMethod(_customMap);
							//List<Object> messages = new ArrayList<>();
							//messages.add(mappingObj);
							//messages.add(methodContainer);
							objStream.writeObject(mappingObj);

							socket.close();
						} catch (SocketException se) {
							se.printStackTrace();
							System.exit(0);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for (int i = 0; i < _numMappers; i++) {
				try {
					process[i].waitFor();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
	    }

		className = "workerPackage.WorkerReduce";//_reducerNodes.getClass().getName();
		{
			Process[] process = new Process[_numReducers];

			for (int i = 0; i < _numReducers; i++) {
				try {
				    ArrayList<String> args = new ArrayList<String>();
					List<String> command = new ArrayList<String>();
					command.add(javaBin);
					//jvmArgs.add(0, String.valueOf(i));
					//command.addAll(jvmArgs);
					command.add("-cp");
					command.add(classpath);
					command.add(className);
					int id = i;
					args.add(0, String.valueOf(id));
					args.add(1, _configFileName);
					args.add(2, String.valueOf(_numMappers));
					args.add(3, String.valueOf(choice));
					command.addAll(args);

					ProcessBuilder builder = new ProcessBuilder(command);
					process[i] = builder.inheritIO().start();
					boolean isConnected = false;
					while (!isConnected) {
						try {
							int port = 4459 + i;
							Socket socket = new Socket("localHost", port);
							System.out.println("Reducer part-Connected master end" + port);
							isConnected = true;

							OutputStream outputStream = socket.getOutputStream();
							ObjectOutputStream objStream = new ObjectOutputStream(outputStream);


							//GenericStreamObjectWrapper methodContainer = new GenericStreamObjectWrapper();
							//methodContainer.setMapMethod(_customMap);
							//List<Object> messages = new ArrayList<>();
							//messages.add(mappingObj);
							//messages.add(methodContainer);
							objStream.writeObject(reducerObject);

							socket.close();
						} catch (SocketException se) {
							se.printStackTrace();
							System.exit(0);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for (int i = 0; i < _numReducers; i++) {
				try {
					process[i].waitFor();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	Object _refObject;
	int _numMappers, _numReducers;
	String _configFileName;
	Method _customMap;
}
