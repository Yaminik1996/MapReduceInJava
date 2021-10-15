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

public class WordCount {

	public void initialize()
	{
		System.out.println("Word count");
		
		String fileName = "tests/config/wordCountConfig.config";
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
		// System.out.println("Map in Controller");
		value = cleanFile(value);
		String[] words = value.split(" ");
		for(String word: words) {
			try {
				emit_intermediate.invoke(_map,word, "1");
				try(OutputStream outputStream = new FileOutputStream("intermediate.properties")){
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
			emit_final.invoke(_reduce, key, result);
			System.out.println(WorkerReduce.reduceProp);

			//Write the final results to an output file
			File file = new File("wordcount.txt");
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			for(Entry<String, Integer> entry : WorkerReduce.reduceProp.entrySet()){
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
	
	private String cleanFile(String value) {
		return value.replaceAll("\\p{Punct}"," ");
	}
	
	Controller _c;
	WorkerMapper _map = new WorkerMapper();
	WorkerReduce _reduce = new WorkerReduce();
}
