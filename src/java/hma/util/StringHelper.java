/**
 * 
 */
package hma.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author guoyezhi
 *
 */
public class StringHelper {
	
	public static String removeDuplicateWhitespace(String inputStr) {
		String patternStr = "\\s+";
		String replaceStr = " ";
		Pattern pattern = Pattern.compile(patternStr);
		Matcher matcher = pattern.matcher(inputStr);
		return matcher.replaceAll(replaceStr);
	}
	
}
