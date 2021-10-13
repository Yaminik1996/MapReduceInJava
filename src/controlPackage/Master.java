package controlPackage;

import java.lang.reflect.Method;
import workerPackage.WorkerMapper;
import workerPackage.WorkerReduce;

public class Master {

	public void initialize(Method customMap, Method customReduce)
	{
		//System.out.println("reached master");
		_mapperNode = new WorkerMapper();
		_mapperNode.initialize(customMap);
		_reducerNode = new WorkerReduce();
		_reducerNode.initialize(customReduce);
	}
	
	public void perform(Object obj)
	{
		System.out.println("Calling from Master");
		_mapperNode.perform(obj);
		_reducerNode.perform(obj);
	}
	
	WorkerMapper _mapperNode;
	WorkerReduce _reducerNode;
}
