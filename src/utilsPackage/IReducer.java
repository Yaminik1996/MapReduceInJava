package utilsPackage;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.List;

import workerPackage.WorkerReduce;

public interface IReducer extends Serializable{
	public void reduce(String key, List<String> values, Method emit_final, WorkerReduce reducerObject);
}
