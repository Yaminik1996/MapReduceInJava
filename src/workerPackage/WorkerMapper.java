package workerPackage;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.io.*;

//This is our Mapper Class
public class WorkerMapper {
	public void initialize(Method customMap, String configFile)
	{
		System.out.println("reached mapper");
		_mapFunction = customMap;
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
		numMappers = prop.getProperty("mappers");
		numReducers = prop.getProperty("reducers");
		inputFile = prop.getProperty("input");
		intermediateFile = prop.getProperty("intermediate");
	
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
	public void perform(Object obj)
	{
		System.out.println("Worker Mapper called");
		try {
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
						_mapFunction.invoke(obj,String.valueOf(Counter), st, this.getClass().getDeclaredMethod("emitIntermediate", emitIntermediateArgs),intermediateFile);
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
	
	Method _mapFunction;
	String numMappers;
	String numReducers;
	String inputFile;
	String intermediateFile;
	public Properties mapProp = new Properties();
	
}
