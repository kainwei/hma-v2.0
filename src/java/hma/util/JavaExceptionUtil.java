/**
 * 
 */
package hma.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author guoyezhi
 *
 */
public class JavaExceptionUtil {
	
	/**
	 * @param e
	 * @return
	 * @throws IOException
	 */
	public static String getExceptionStackTraceMessage(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		pw.flush();
		sw.flush();
		return sw.toString();
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		System.out.println(
				JavaExceptionUtil.getExceptionStackTraceMessage(
						new RuntimeException("hehe")));
	}
	
}
