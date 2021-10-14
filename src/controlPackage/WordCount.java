package controlPackage;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class WordCount {

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
		value = cleanFile(value);
		String[] words = value.split(" ");
		for(String word: words) {
			try {
				emit_intermediate.invoke(word, 1);
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
		Integer result = 0;
		for(String v: values) {
	           result += Integer.valueOf(v);
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
	
	private String cleanFile(String value) {
		return value.replaceAll("\\p{Punct}"," ");
	}
	
	Controller _c;
}
