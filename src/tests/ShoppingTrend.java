package tests;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import controlPackage.Controller;
import workerPackage.WorkerMapper;
import workerPackage.WorkerReduce;

public class ShoppingTrend {

	public void initialize()
	{
		String fileName = "tests/config/shoppingTrendConfig.config";
		_c = new Controller();
		Class[] mapArgs = {String.class, String.class, Method.class, String.class};
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


	public void methodMap(String key, String value, Method emit_intermediate, String intermediateFile)
	{
		System.out.println("Map in Controller");
		String[] entries = value.split(";");
		for(String entry: entries) {
			String[] parts = entry.split(",");
			Integer id = Integer.valueOf(parts[0]);
			Integer cost = (int) Math.round(Double.valueOf(parts[1]));
			Integer quantity = Integer.valueOf(parts[2]);
			try {
				emit_intermediate.invoke(_map,String.valueOf(id), String.valueOf(cost*quantity));
				try(OutputStream outputStream = new FileOutputStream(intermediateFile)){
					_map.mapProp.store(outputStream,null);
				} catch (IOException e) {
					e.printStackTrace();
				} 
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
			emit_final.invoke(_reduce,key, result);
			//Write the final results to an output file
			File file = new File("shoppingTrends.txt");
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			for(java.util.Map.Entry<String, Integer> entry : WorkerReduce.reduceProp.entrySet()){
				bw.write( entry.getKey() + ":" + entry.getValue() );
                
                //new line
                bw.newLine();
			}
			bw.flush();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	Controller _c;
	WorkerMapper _map = new WorkerMapper();
	WorkerReduce _reduce = new WorkerReduce();
}
