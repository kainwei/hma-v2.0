/**
 * 
 */
package hma.monitor.strategy.trigger.task;


/**
 * @author guoyezhi
 *
 */
public abstract class MonitorStrategyAttachedTask implements Runnable {
	
	private String taskWorker = null;
	
	private String[] workerArgs = null;
	
	private MonitorStrategyAttachedTaskArguments args = null;
	
	
	/**
	 * @param taskWorker
	 * @param workerArgs
	 * @param args
	 */
	public MonitorStrategyAttachedTask(
			String taskWorker,
			String[] workerArgs,
			MonitorStrategyAttachedTaskArguments args) {
		this.taskWorker = taskWorker;
		this.workerArgs = workerArgs;
		this.args = args;
	}
	
	
	/**
	 * @param taskWorker the taskWorker to set
	 */
	public void setTaskWorker(String taskWorker) {
		this.taskWorker = taskWorker;
	}

	/**
	 * @return the taskWorker
	 */
	public String getTaskWorker() {
		return taskWorker;
	}

	/**
	 * @param workerArgs the workerArgs to set
	 */
	public void setWorkerArgs(String[] workerArgs) {
		this.workerArgs = workerArgs;
	}

	/**
	 * @return the workerArgs
	 */
	public String[] getWorkerArgs() {
		return workerArgs;
	}
	
	/**
	 * @param args the args to set
	 */
	public void setAttachedTaskArguments(MonitorStrategyAttachedTaskArguments args) {
		this.args = args;
	}

	/**
	 * @return the args
	 */
	public MonitorStrategyAttachedTaskArguments getAttachedTaskArguments() {
		return args;
	}
	
}
