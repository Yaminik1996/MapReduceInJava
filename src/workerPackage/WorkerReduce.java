package workerPackage;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class WorkerReduce {
	public void initialize(Method customReduce)
	{
		System.out.println("reached reducer");
		_reduceFunction = customReduce;
		
		String fileName = "intermediate.properties";
		try (FileInputStream fis = new FileInputStream(fileName)) {
			mapProp.load(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//Function to print key value pair from intermediate file
	public static void printProperties(Properties prop)
    {	
        for (Object key: prop.keySet()) {
            System.out.println(key + ": " + prop.getProperty(key.toString()));
        }
    }

	public static void emitFinal(String key, List<String> value){
		reduceProp.put(key,value);

	}
	public void perform(Object obj)
	{
		System.out.println("Worker Reduce called");
		Class[] emitFinalArgs = {String.class, List.class};
		try {
			for (Object key: mapProp.keySet()) {
				System.out.println(key + ": " + mapProp.getProperty(key.toString()));
				String[] values = mapProp.getProperty(key.toString()).split(",");
				System.out.println(values);
				_reduceFunction.invoke(obj, String.valueOf(key), values, this.getClass().getDeclaredMethod("emitFinal", emitFinalArgs));
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
	}
	
	Method _reduceFunction;
	public Properties mapProp = new Properties();
	public static Hashtable<String, List<String>> reduceProp = new Hashtable<String,List<String>>();
}
