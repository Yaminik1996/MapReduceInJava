package workerPackage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public class WorkerReduce {
	String configFile = "";
	public void initialize(Method customReduce, String configFile)
	{
		System.out.println("reached reducer");
		_reduceFunction = customReduce;
		this.configFile = configFile;
		_reducerId = 0;
	}

	//Function to print key value pair from intermediate file
	public static void printProperties(Properties prop)
    {	
        for (Object key: prop.keySet()) {
            System.out.println(key + ": " + prop.getProperty(key.toString()));
        }
    }

	// Emit Final function to store the final key value pair
	public static void emitFinal(String key, Integer value){
		reduceProp.put(key,value);
	}
	
	public void perform(Object obj)
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
		
		Properties mapProperties[] = new Properties[_numFiles];

		String dirName = System.getProperty("user.dir") + "\\"+_reducerId;
		String intermediateFile = prop.getProperty("intermediate");
		
		for (int i =0; i<_numFiles; i++)
		{
			String newIntermediateFileName = dirName +"\\"+ i + intermediateFile;
			
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
			
			if (i == _numFiles-1){
				File correspondingDir = new File(dirName);
				correspondingDir.delete();
			}
		}
		
		Class[] emitFinalArgs = {String.class, Integer.class};
		try {
			for (Object key: mapProp.keySet()) {
				System.out.println(key + ": " + mapProp.getProperty(key.toString()));
				List<String> values = Arrays.stream(mapProp.getProperty(key.toString()).split(",")).collect(Collectors.toList());
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

		System.out.println("done");
	}
	
	Method _reduceFunction;
	int _reducerId=0;
	int _numFiles=1;
	public Properties mapProp = new Properties();
	public static Hashtable<String, Integer> reduceProp = new Hashtable<String, Integer>();
}
