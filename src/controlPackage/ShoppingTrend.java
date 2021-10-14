package controlPackage;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class ShoppingTrend {

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


	public void methodMap(String key, String value, Method emit_intermediate)
	{
		System.out.println("Map in Controller");
		String[] entries = value.split(";");
		for(String entry: entries) {
			String[] parts = entry.split(",");
			Integer id = Integer.valueOf(parts[0]);
			Double cost = Double.valueOf(parts[1]);
			Integer quantity = Integer.valueOf(parts[2]);
			try {
				emit_intermediate.invoke(id, cost*quantity);
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
	}

	public void methodReduce(String key, List<String> values, Method emit_final)
	{
		System.out.println("Reduce in Controller");
		Double result = 0.0;
		for(String v: values) {
	           result += Double.valueOf(v);
		}
		try {
			emit_final.invoke(key, result);
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
	
	Controller _c;
}
