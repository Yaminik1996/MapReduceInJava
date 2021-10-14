package controlPackage;
import java.lang.reflect.Method;

public class Controller {

	public void initialize(Method customMap, Method customReduce)
	{
		_masterNode = new Master();
		_masterNode.initialize(customMap, customReduce);
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
