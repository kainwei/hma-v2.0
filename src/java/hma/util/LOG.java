package hma.util;

public class LOG {

	public static void fatal(String str) {
		System.err.println(str);
	}

	public static void warn(String str) {
		System.out.println(str);
	}

	public static void warn(String str, Exception e) {
		System.out.println(str + e.getMessage());
		e.printStackTrace();
	}

	public static void info(String str) {
		System.out.println(str);
	}

	public static void debug(String str) {
		System.out.println(str);
	}
}
