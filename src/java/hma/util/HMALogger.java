/**
 * 
 */
package hma.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @author guoyezhi
 *
 */
public class HMALogger {
	
	private File logFile = null;
	
	public HMALogger(String logName) {
		this.logFile = new File(logName);
	}
	
	public void log(String content) {
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(
					new FileWriter(logFile, true));
			writer.println(content);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			writer.close();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
