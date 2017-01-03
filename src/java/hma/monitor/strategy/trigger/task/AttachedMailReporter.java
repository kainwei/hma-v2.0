/**
 * 
 */
package hma.monitor.strategy.trigger.task;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;

import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

/**
 * @author guoyezhi
 *
 */
public class AttachedMailReporter extends MonitorStrategyAttachedTask {
	
	private String hostname = "127.0.0.1";
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * @param taskWorker
	 * @param workerArgs
	 * @param taskArgs
	 */
	public AttachedMailReporter(
			String taskWorker,
			String[] workerArgs,
			MonitorStrategyAttachedTaskArguments taskArgs) {
		super(taskWorker, workerArgs, taskArgs);
		BufferedReader read = null;
		try {
			Process proc = Runtime.getRuntime().exec("hostname");
			read = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			hostname = read.readLine();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			try {
				read.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void doSendMail(String[] receivers, String subject, String body) {
    	
    	try {
			SimpleEmail email = new SimpleEmail();			
			for (int i = 0; i < receivers.length; i++){
				email.addTo(receivers[i]);
			}
			email.setCharset("gbk");
			//email.setFrom(System.getProperty("user.name") + "@"+ hostname);
			String mfrom = "boyan5@staff.sina.com.cn";
			email.setFrom(mfrom);
			email.setAuthentication("boyan5@staff.sina.com.cn", "ddzs%%017");
			email.setHostName("mail.staff.sina.com.cn");
			email.setSubject(subject);
			email.setMsg(body);
			email.send();
		} catch (EmailException ee) {
			String tmp = "";
			for (int i = 0; i < receivers.length; i++){
				tmp = tmp + receivers[i];
			}
			System.err.println("EmailException: mail is = " + tmp);
			throw new RuntimeException(ee);
		}
		
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		
		String[] receivers = getWorkerArgs();
		MonitorStrategyAttachedTaskArguments taskArgs =
			getAttachedTaskArguments();
		
		StringBuilder subjectBuilder = new StringBuilder();
		subjectBuilder.append("[" + taskArgs.getTargetSystemName() + "]");
		subjectBuilder.append("[" + taskArgs.getAlarmLevel() + "]");
		subjectBuilder.append("[" + taskArgs.getMonitorStrategyName() + "]");
		subjectBuilder.append("[" + taskArgs.getKeyInfo() + "]");
		subjectBuilder.append("[" + dateFormat.format(taskArgs.getTimestamp()) + "]");
		
		StringBuilder bodyBuilder = new StringBuilder();
		bodyBuilder.append(subjectBuilder.toString() + "\n\n");
		bodyBuilder.append(taskArgs.getFullInfo());
		
		doSendMail(receivers, subjectBuilder.toString(), bodyBuilder.toString());
	}
	
}

