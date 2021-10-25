package controlPackage;

import java.lang.reflect.Method;

import tests.MovieRating;

public class Controller3 {

	public void initialize(Method customMap, Method customReduce, String configFile)
	{
		System.out.println(configFile);
		_masterNode = new Master();
		_masterNode.initialize(customMap, customReduce, configFile);
	}
	
	public void perform(Object obj)
	{
		_masterNode.perform(obj);		
	}
	
	public static void main(String[] args) {
		System.out.println("From Controller-main");
		movieRating = new MovieRating();
		movieRating.initialize();
	}
	
	Master _masterNode;
	static MovieRating movieRating;
}
