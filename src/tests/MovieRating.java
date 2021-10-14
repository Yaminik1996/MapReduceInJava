package tests;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import controlPackage.Controller;

public class MovieRating {

	public void initialize()
	{
		String fileName = "config/movieRatingConfig.conf";
		_c = new Controller();
		try {
			_c.initialize(this.getClass().getDeclaredMethod("methodMap", null), this.getClass().getDeclaredMethod("methodReduce", null), fileName);
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
		String[] movies = value.split(";");
		for(String movie: movies) {
			String[] parts = movie.split(":");
			String movieName = parts[0];
			Integer rating = Integer.valueOf(parts[1]);
			try {
				emit_intermediate.invoke(movieName, rating);
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
		Integer count = 0;
		Integer totalRating = 0;
		for(String v: values) {
	           totalRating += Integer.valueOf(v);
	           ++count;
		}
		try {
			emit_final.invoke(key, totalRating/count);
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
