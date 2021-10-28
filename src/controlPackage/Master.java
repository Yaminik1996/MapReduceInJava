package controlPackage;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;

import workerPackage.WorkerMapper;
import workerPackage.WorkerReduce;

public class Master {

	public void initialize(Method customMap, Method customReduce, String configFile)
	{
		System.out.println("reached master");
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
		_numMappers = Integer.parseInt(prop.getProperty("mappers"));
		_numReducers = Integer.parseInt(prop.getProperty("reducers"));

		// create mapper N times and initialize
		
		// create reducer M times and initialize
		
		_mapperNode = new WorkerMapper();
		_mapperNode.initialize(customMap,configFile);
		_reducerNode = new WorkerReduce();
		_reducerNode.initialize(customReduce,configFile);
	}
	
	public void perform(Object obj)
	{
		System.out.println("Calling from Master");
		_mapperNode.perform(obj);
		_reducerNode.perform(obj);
	}
	
	int _numMappers=1, _numReducers=1;
	WorkerMapper _mapperNode;
	WorkerReduce _reducerNode;
}
