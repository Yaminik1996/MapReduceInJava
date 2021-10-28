package tests;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.KeyStore.Entry;
import java.util.List;
import java.io.FileOutputStream;

import controlPackage.Controller;
import workerPackage.WorkerMapper;
import workerPackage.WorkerReduce;

public class MovieRating {

	public void initialize()
	{
		String fileName = "tests/config/movieRatingConfig.config";
		//for eclipse debugging
		//fileName = "src/"+fileName;
		
		_c = new Controller();
		Class[] mapArgs = {String.class, String.class, Method.class, String.class, Integer.class, Integer.class};
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


	public void methodMap(String key, String value, Method emit_intermediate, String intermediateFile, Integer numFilesToEmit, Integer mapperId)
	{
		System.out.println("Map in Controller");
		String[] movies = value.split(";");

		double numWords = movies.length;
		double numWordsPerFile = numWords/numFilesToEmit;
		int numWordsPerPartition = (int) Math.ceil(numWordsPerFile);
		int counter = 0;
		for(String movie: movies) {
			String dirName = System.getProperty("user.dir") + "/"+counter/numWordsPerPartition; 
			File correspondingDir = new File(dirName);
			if (!correspondingDir.exists()){
				correspondingDir.mkdir();
			}

			int idToPut = mapperId;
			String newintermediateFile = dirName +"/" + idToPut + intermediateFile;

			String[] parts = movie.split(":");
			String movieName = parts[0];
			Integer rating = Integer.valueOf(parts[1]);
			try {
				emit_intermediate.invoke(_map,movieName, String.valueOf(rating));
				try(OutputStream outputStream = new FileOutputStream(newintermediateFile)){
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
			counter++;
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
			emit_final.invoke(_reduce, key, totalRating/count);
			System.out.println(WorkerReduce.reduceProp);

			//Write the final results to an output file
			File file = new File("movierating.txt");
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
			e.printStackTrace();
		}
	}
	
	Controller _c;
	WorkerMapper _map = new WorkerMapper();
	WorkerReduce _reduce = new WorkerReduce();
}
