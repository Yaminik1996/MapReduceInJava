package controlPackage;

import java.lang.reflect.Method;

import tests.MovieRating;
import tests.ShoppingTrend;
import tests.WordCount;

public class Controller2 {

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
		shoppingTrend = new ShoppingTrend();
		shoppingTrend.initialize();
	}
	
	Master _masterNode;
	static WordCount wordCount;
	static ShoppingTrend shoppingTrend;
	static MovieRating movieRating;
}
