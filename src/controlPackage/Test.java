package controlPackage;

import java.lang.reflect.Method;

public class Test {

	public void initialize()
	{
		_c = new Controller();
		try {
			_c.initialize(this.getClass().getDeclaredMethod("methodMap", null), this.getClass().getDeclaredMethod("methodReduce", null));
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		_c.perform(this);
	}


	public void methodMap()
	{
		System.out.println("Map in Controller");
	}
	public void methodReduce()
	{
		System.out.println("Reduce in Controller");
	}
	
	Controller _c;
}
