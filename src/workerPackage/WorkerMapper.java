package workerPackage;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class WorkerMapper {
	public void initialize(Method customMap)
	{
		//System.out.println("reached mapper");
		_mapFunction = customMap;
	}
	
	public void perform(Object obj)
	{
		System.out.println("Worker Mapper called");
		try {
			_mapFunction.invoke(obj);
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	Method _mapFunction;
}
