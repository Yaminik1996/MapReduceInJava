package controlPackage;

import java.lang.reflect.Method;

import tests.MovieRating;
import tests.ShoppingTrend;
import tests.WordCount;

public class Controller {

	public void initialize(Method customMap, Method customReduce, String configFile)
	{
		_masterNode = new Master();
		_masterNode.initialize(customMap, customReduce, configFile);
	}
	
	public void perform(Object obj)
	{
		_masterNode.perform(obj);		
	}
	
	public static void main(String[] args) {
		System.out.println("From Controller-main");
		wordCount = new WordCount();
		wordCount.initialize();
		shoppingTrend = new ShoppingTrend();
		shoppingTrend.initialize();
		movieRating = new MovieRating();
		movieRating.initialize();
	}
	
	Master _masterNode;
	static WordCount wordCount;
	static ShoppingTrend shoppingTrend;
	static MovieRating movieRating;
}
