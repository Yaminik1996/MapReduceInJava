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
import utilsPackage.IMapper;
import utilsPackage.IReducer;
import workerPackage.WorkerMapper;
import workerPackage.WorkerReduce;

class Mapper2 implements IMapper {

	@Override
	public void map(String key, String value, Method emit_intermediate, String intermediateFile, Integer numFilesToEmit,
			Integer mapperId, WorkerMapper mapperObject) {
		System.out.println("Map in Controller");
		String[] entries = value.split(";");

		double numWords = entries.length;
		double numWordsPerFile = numWords / numFilesToEmit;
		int numWordsPerPartition = (int) Math.ceil(numWordsPerFile);
		int counter = 0;
		for (String entry : entries) {
			String dirName = System.getProperty("user.dir") + "/" + counter / numWordsPerPartition;
			File correspondingDir = new File(dirName);
			if (!correspondingDir.exists()) {
				correspondingDir.mkdir();
			}

			int idToPut = mapperId;
			String newintermediateFile = dirName + "/" + idToPut + intermediateFile;

			String[] parts = entry.split(",");
			Integer id = Integer.valueOf(parts[0]);
			Integer cost = (int) Math.round(Double.valueOf(parts[1]));
			Integer quantity = Integer.valueOf(parts[2]);
			try {
				emit_intermediate.invoke(mapperObject, String.valueOf(id), String.valueOf(cost * quantity));
				try (OutputStream outputStream = new FileOutputStream(newintermediateFile)) {
					mapperObject.mapProp.store(outputStream, null);
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
			counter++;
		}
	}
	
}

class Reducer2 implements IReducer{

	@Override
	public void reduce(String key, List<String> values, Method emit_final, WorkerReduce reducerObject) {
		System.out.println("Reduce in Controller");
		Integer result = 0;
		for (String v : values) {
			result += Integer.valueOf(v);
		}
		try {
			emit_final.invoke(reducerObject, key, result);
			// Write the final results to an output file
			File file = new File("shoppingTrends.txt");
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			for (java.util.Map.Entry<String, Integer> entry : WorkerReduce.reduceProp.entrySet()) {
				bw.write(entry.getKey() + ":" + entry.getValue());
				// new line
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
}

public class ShoppingTrend {

	public void initialize()
	{
		String fileName = "tests/config/shoppingTrendConfig.config";
		//for eclipse debugging
		//fileName = "src/"+fileName;
		
		_c = new Controller();
		Class[] mapArgs = {String.class, String.class, Method.class, String.class, Integer.class, Integer.class, WorkerMapper.class};
		Class[] reduceArgs = {String.class, List.class, Method.class, WorkerReduce.class};
		Mapper2 mapObj = new Mapper2();
		Reducer2 redObj = new Reducer2();
		try {
			_c.initialize(mapObj.getClass().getDeclaredMethod("map", mapArgs), redObj.getClass().getDeclaredMethod("reduce", reduceArgs), fileName, this);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}
		_c.perform(mapObj, redObj);
	}


	
	Controller _c;
}
