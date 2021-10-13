package workerPackage;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class WorkerReduce {
	public void initialize(Method customReduce)
	{
		_reduceFunction = customReduce;
	}
	
	public void perform(Object obj)
	{
		System.out.println("Worker Reduce called");
		try {
			_reduceFunction.invoke(obj);
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
	
	Method _reduceFunction;
}
