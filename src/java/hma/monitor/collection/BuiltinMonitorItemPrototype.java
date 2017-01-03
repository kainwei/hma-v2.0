/**
 * 
 */
package hma.monitor.collection;

/**
 * @author guoyezhi
 * 
 */
public class BuiltinMonitorItemPrototype extends MonitorItemPrototype {
	
	private Class<?> workerClass = null;
	
	public BuiltinMonitorItemPrototype(
			String name, 
			String type, 
			long period,
			String description, 
			String className, 
			String rawDataTypeName) {
		super(name, type, period, description, rawDataTypeName);
		this.setMonitorItemWorkerClass(className);
	}
	
	public void setMonitorItemWorkerClass(String className) {
		try {
			workerClass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public Class<?> getMonitorItemWorkerClass() {
		return workerClass;
	}
	
}
