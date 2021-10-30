package controlPackage;

import java.lang.reflect.Method;
import tests.WordCount;

public class Controller {

	public void initialize(Method customMap, Method customReduce, String configFile, Object objRef)
	{
		System.out.println(configFile);
		_masterNode = new Master();
		_masterNode.initialize(customMap, customReduce, configFile, objRef);
	}
	
	public void perform(Object mappingObj, Object reducerObject)
	{
		_masterNode.perform(mappingObj, reducerObject, 1);		
	}
	
	public static void main(String[] args) {
		System.out.println("From Controller-main");

		wordCount = new WordCount();
		wordCount.initialize();
	}
	
	Master _masterNode;
	static WordCount wordCount;
}
