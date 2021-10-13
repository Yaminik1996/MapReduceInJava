package controlPackage;

import workerPackage.WorkerMapper;
import workerPackage.WorkerReduce;

public class Master {

	public void invokeWorker()
	{
		WorkerMapper w1 = new WorkerMapper();
		WorkerReduce w2 = new WorkerReduce();
		System.out.println("Calling from Master");
		w1.callMe();
		w2.callMe();
		
	}
}
