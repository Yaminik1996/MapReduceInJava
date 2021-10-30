package utilsPackage;

import java.io.Serializable;
import java.lang.reflect.Method;

import workerPackage.WorkerMapper;

public interface IMapper extends Serializable{
	public void map(String key, String value, Method emit_intermediate,String intermediateFile, Integer numFilesToEmit, Integer mapperId,  WorkerMapper mapperObject);
}
