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
import java.lang.Math;

class Mapper implements IMapper {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9076494750479877108L;
	//Method _methodMap;
	
	
	private String cleanFile(String value) {
		return value.replaceAll("\\p{Punct}"," ");
	}
	
	@Override
	public void map(String key, String value, Method emit_intermediate, String intermediateFile, Integer numFilesToEmit, Integer mapperId, WorkerMapper mapperObject) {
		// Integer numFilesToEmit, Integer mapperId
		// TODO Auto-generated method stub
		// System.out.println("Map in Controller");
		value = cleanFile(value);
		String[] words = value.split(" ");

		double numWords = words.length;
		double numWordsPerFile = numWords / numFilesToEmit;
		int numWordsPerPartition = (int) Math.ceil(numWordsPerFile);
		int counter = 0;
		for (String word : words) {
			String dirName = System.getProperty("user.dir") + "/" + counter / numWordsPerPartition;
			File correspondingDir = new File(dirName);
			if (!correspondingDir.exists()) {
				correspondingDir.mkdir();
			}

			int idToPut = mapperId;
			String newintermediateFile = dirName + "/" + idToPut + intermediateFile;
			//System.out.println(newintermediateFile);
			//System.out.println(word);
			try {
				emit_intermediate.invoke(mapperObject, word, "1");
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

class Reducer implements IReducer
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7562978929119239598L;
	
	@Override
	public void reduce(String key, List<String> values, Method emit_final, WorkerReduce reducerObject) {
		// TODO Auto-generated method stub
		// System.out.println("Reduce in Controller");
		Integer result = 0;
		for (String v : values) {
			result += Integer.valueOf(v);
		}
		try {
			emit_final.invoke(reducerObject, key, result);
			// System.out.println(WorkerReduce.reduceProp);

			// Write the final results to an output file
			File file = new File("wordcount.txt");
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			for (Entry<String, Integer> entry : WorkerReduce.reduceProp.entrySet()) {
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

public class WordCount {

	public void initialize()
	{
		System.out.println("Word count");
		
		String fileName = "tests/config/wordCountConfig.config";
		//for eclipse debugging 
		//fileName = "src/"+fileName;

		_c = new Controller();
		Class[] mapArgs = {String.class, String.class, Method.class, String.class, Integer.class, Integer.class, WorkerMapper.class};
		Class[] reduceArgs = {String.class, List.class, Method.class, WorkerReduce.class};
		Mapper mapObj = new Mapper();
		Reducer redObj = new Reducer();
		try {
			_c.initialize(mapObj.getClass().getDeclaredMethod("map", mapArgs ), redObj.getClass().getDeclaredMethod("reduce", reduceArgs), fileName, this);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}
		_c.perform(mapObj, redObj);
	}

	Controller _c;
}
