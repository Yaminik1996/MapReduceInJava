package tests;

import java.lang.reflect.InvocationTargetException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

import controlPackage.Controller;

public class ShoppingTrend {

	public void initialize()
	{
		String fileName = "tests/config/shoppingTrendConfig.config";
		_c = new Controller();
		Class[] mapArgs = {String.class, String.class, Method.class};
		Class[] reduceArgs = {String.class, List.class, Method.class};
		try {
			_c.initialize(this.getClass().getDeclaredMethod("methodMap", mapArgs ), this.getClass().getDeclaredMethod("methodReduce", reduceArgs), fileName);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
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
				
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				
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
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
	
	Controller _c;
}
