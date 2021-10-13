package controlPackage;

public class Controller {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("From Controller-main");
		Master m = new Master();
		m.invokeWorker();
	}

}
