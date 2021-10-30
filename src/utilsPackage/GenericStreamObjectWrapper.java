package utilsPackage;
import java.io.Serializable;
import java.lang.reflect.Method;

public class GenericStreamObjectWrapper implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8927625697253353922L;
	//private static final long serialVersionUID = 1L;
	//Object _callerObject;
	Method _mapMethod;
	
	public Method getMapMethod()
	{
		return _mapMethod;
	}
	
	public void setMapMethod(Method mapper)
	{
		_mapMethod = mapper;
	}
}
