package controlPackage;

import java.lang.reflect.Method;

import tests.MovieRating;

public class Controller3 {

	public void initialize(Method customMap, Method customReduce, String configFile, Object obj)
	{
		System.out.println(configFile);
		_masterNode = new Master();
		_masterNode.initialize(customMap, customReduce, configFile, obj);
	}
	
	public void perform(Object mapper, Object reducer)
	{
		_masterNode.perform(mapper, reducer, 3);		
	}
	
	public static void main(String[] args) {
		System.out.println("From Controller-main");
		movieRating = new MovieRating();
		movieRating.initialize();
	}
	
	Master _masterNode;
	static MovieRating movieRating;
}
