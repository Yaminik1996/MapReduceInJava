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
		// TODO Auto-generated method stub
		System.out.println("From Controller-main");
		test = new Test();
		test.initialize();
	}
	
	Master _masterNode;
	static Test test;
}
