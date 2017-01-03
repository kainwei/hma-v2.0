/**
 * 
 */
package hma.monitor.strategy.trigger.task;

/**
 * @author guoyezhi
 *
 */
public class AttachedCustomizedWorker extends MonitorStrategyAttachedTask {

	/**
	 * @param taskWorker
	 * @param workerArgs
	 * @param args
	 */
	public AttachedCustomizedWorker(
			String taskWorker,
			String[] workerArgs,
			MonitorStrategyAttachedTaskArguments args) {
		super(taskWorker, workerArgs, args);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
