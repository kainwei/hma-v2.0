package hma.conf;

import hma.util.LOG;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;


/**
 * @author Vergil
 *
 */
public class Configuration implements Iterable<Map.Entry<String,String>> {

	/**
	 * List of configuration resources.
	 */
	private List<Object> resources = new ArrayList<Object>();
	
	private Properties properties;
	
	//private boolean quietmode = true;
	private boolean quietmode = false;
	
	private String propertyNameTagString = "name";
	private String propertyValueTagString = "value";
	

	private ClassLoader classLoader;
	{
		classLoader = Thread.currentThread().getContextClassLoader();
		if (classLoader == null) {
			classLoader = Configuration.class.getClassLoader();
		}
	}
	
	
	private String moduleName = null;
	private boolean loadDefaults = true;
	
	/** A new configuration. */
	public Configuration() {
		this.initConfigurationResources();
	}

	/**
	 * A new configuration where the behavior of reading from the default 
	 * resources can be turned off.
	 * 
	 * If the parameter {@code loadDefaults} is false, the new instance
	 * will not load resources from the default files. 
	 * @param loadDefaults specifies whether to load from the default files
	 */
	public Configuration(boolean loadDefaults) {
		this.loadDefaults = loadDefaults;
		this.initConfigurationResources();
	}
	
	public Configuration(boolean loadDefaults, String moduleName) {
		this.loadDefaults = loadDefaults;
		this.moduleName = moduleName;
		this.initConfigurationResources();
	}
	
	private synchronized void initConfigurationResources() {
		if ((this.moduleName != null) && this.loadDefaults) {
			resources.add(moduleName + "-default.xml");
			//resources.add(moduleName + "-site.xml");
			//resources.add("D:\\eclipse\\guoyezhi\\workspace\\HMA\\conf\\hma-default.xml");
		}
	}
	
	public synchronized void reinitConfigurationResources() {
		resources.clear();
		if ((this.moduleName != null) && this.loadDefaults) {
			resources.add(moduleName + "-default.xml");
			resources.add(moduleName + "-site.xml");
			//resources.add("D:\\eclipse\\guoyezhi\\workspace\\HMA\\src\\monitor-items-site.xml");
		}
		this.reloadConfiguration();
	}
	
	private synchronized void addConfigurationResourceObject(Object resource) {
		resources.add(resource); // add to resources
		reloadConfiguration();
	}
	
	/**
	 * Add a configuration resource.
	 * 
	 * The properties of this resource will override properties of previously
	 * added resources, unless they were marked <a href="#Final">final</a>.
	 * 
	 * @param name
	 *            resource to be added, the classpath is examined for a file
	 *            with that name.
	 */
	public void addConfigurationResource(String name) {
		addConfigurationResourceObject(name);
	}
	
	public void addConfigurationResource(File file) {
		addConfigurationResourceObject(file);
	}
	
	/**
	 * Reload configuration from previously added resources.
	 *
	 * This method will clear all the configuration read from the added 
	 * resources, and final parameters. This will make the resources to 
	 * be read again before accessing the values. Values that are added
	 * via set methods will overlay values read from the resources.
	 */
	public synchronized void reloadConfiguration() {
		properties = null;                            // trigger reload
	}
	
	public synchronized void setModuleName(String moduleName) {
		if (moduleName != null)
			this.moduleName = moduleName;
		this.reinitConfigurationResources();
	}
	
	
	protected synchronized List<Object> getResources() {
		return resources;
	}
	
	protected synchronized void setResources(List<Object> resources) {
		this.resources = resources;
	}
	
	public synchronized boolean getQuietMode() {
		return quietmode;
	}
	
	/** 
	 * Set the quiteness-mode. 
	 * 
	 * In the the quite-mode error and informational messages might not be logged.
	 * 
	 * @param quietmode <code>true</code> to set quiet-mode on, <code>false</code>
	 *              to turn it off.
	 */
	public synchronized void setQuietMode(boolean quietmode) {
		this.quietmode = quietmode;
	}
	
	public synchronized String getPropertyNameTagString() {
		return propertyNameTagString;
	}
	
	public synchronized void setPropertyNameTagString(String nameTagStr) {
		this.propertyNameTagString = nameTagStr;
	}
	
	public synchronized String getPropertyValueTagString() {
		return propertyValueTagString;
	}
	
	public synchronized void setPropertyValueTagString(String valueTagStr) {
		this.propertyValueTagString = valueTagStr;
	}
	
	
	private static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
	private static int MAX_SUBST = 40;

	private String substituteVars(String expr) {
		if (expr == null) {
			return null;
		}
		Matcher match = varPat.matcher("");
		String eval = expr;
		for(int s=0; s<MAX_SUBST; s++) {
			match.reset(eval);
			if (!match.find()) {
				return eval;
			}
			String var = match.group();
			var = var.substring(2, var.length()-1); // remove ${ .. }
			String val = null;
			try {
				val = System.getProperty(var);
			} catch(SecurityException se) {
				LOG.warn("Unexpected SecurityException in Configuration", se);
			}
			if (val == null) {
				val = getRaw(var);
			}
			if (val == null) {
				return eval; // return literal ${var}: var is unbound
			}
			// substitute
			eval = eval.substring(0, match.start()) + val + eval.substring(match.end());
		}
		throw new IllegalStateException("Variable substitution depth too large: " 
				+ MAX_SUBST + " " + expr);
	}
	
	/**
	 * Get the value of the <code>name</code> property, <code>null</code> if
	 * no such property exists.
	 * 
	 * Values are processed for <a href="#VariableExpansion">variable expansion</a> 
	 * before being returned. 
	 * 
	 * @param name the property name.
	 * @return the value of the <code>name</code> property, 
	 *         or null if no such property exists.
	 */
	public String get(String name) {
		return substituteVars(getProps().getProperty(name));
	}

	/** 
	 * Get the value of the <code>name</code> property. If no such property 
	 * exists, then <code>defaultValue</code> is returned.
	 * 
	 * @param name property name.
	 * @param defaultValue default value.
	 * @return property value, or <code>defaultValue</code> if the property 
	 *         doesn't exist.                    
	 */
	public String get(String name, String defaultValue) {
		String res = substituteVars(getProps().getProperty(name, defaultValue));
		if (res == null) {
			System.err.println("Vergil DEBUG: ${" + 
				name + "} not found!");
		}
		return res;
	}
	
	/**
	 * Get the comma delimited values of the <code>name</code> property as an
	 * array of <code>String</code>s. If no such property is specified then
	 * <code>null</code> is returned.
	 * 
	 * @param name
	 *            property name.
	 * @return property value as an array of <code>String</code>s, or
	 *         <code>null</code>.
	 */
	public String[] getStrings(String name) {
		String valueString = get(name);
		if (valueString == null)
			return null;
		return valueString.split(",");
	}

	/**
	 * Get the value of the <code>name</code> property, without doing
	 * <a href="#VariableExpansion">variable expansion</a>.
	 * 
	 * @param name the property name.
	 * @return the value of the <code>name</code> property, 
	 *         or null if no such property exists.
	 */
	public String getRaw(String name) {
		return getProps().getProperty(name);
	}
	
	private String getHexDigits(String value) {
		boolean negative = false;
		String str = value;
		String hexString = null;
		if (value.startsWith("-")) {
			negative = true;
			str = value.substring(1);
		}
		if (str.startsWith("0x") || str.startsWith("0X")) {
			hexString = str.substring(2);
			if (negative) {
				hexString = "-" + hexString;
			}
			return hexString;
		}
		return null;
	}
	
	/** 
	 * Get the value of the <code>name</code> property as an <code>int</code>.
	 *   
	 * If no such property exists, or if the specified value is not a valid
	 * <code>int</code>, then <code>defaultValue</code> is returned.
	 * 
	 * @param name property name.
	 * @param defaultValue default value.
	 * @return property value as an <code>int</code>, 
	 *         or <code>defaultValue</code>. 
	 */
	public int getInt(String name, int defaultValue) {
		String valueString = get(name);
		if (valueString == null)
			return defaultValue;
		try {
			String hexString = getHexDigits(valueString);
			if (hexString != null) {
				return Integer.parseInt(hexString, 16);
			}
				return Integer.parseInt(valueString);
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}
	
	/** 
	 * Get the value of the <code>name</code> property as a <code>long</code>.  
	 * If no such property is specified, or if the specified value is not a valid
	 * <code>long</code>, then <code>defaultValue</code> is returned.
	 * 
	 * @param name property name.
	 * @param defaultValue default value.
	 * @return property value as a <code>long</code>, 
	 *         or <code>defaultValue</code>. 
	 */
	public long getLong(String name, long defaultValue) {
		String valueString = get(name);
		if (valueString == null)
			return defaultValue;
		try {
			String hexString = getHexDigits(valueString);
			if (hexString != null) {
				return Long.parseLong(hexString, 16);
			}
			return Long.parseLong(valueString);
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	/** 
	 * Get the value of the <code>name</code> property as a <code>float</code>.  
	 * If no such property is specified, or if the specified value is not a valid
	 * <code>float</code>, then <code>defaultValue</code> is returned.
	 * 
	 * @param name property name.
	 * @param defaultValue default value.
	 * @return property value as a <code>float</code>, 
	 *         or <code>defaultValue</code>. 
	 */
	public float getFloat(String name, float defaultValue) {
		String valueString = get(name);
		if (valueString == null)
			return defaultValue;
		try {
			return Float.parseFloat(valueString);
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}
	
	/** 
	 * Get the value of the <code>name</code> property as a <code>boolean</code>.  
	 * If no such property is specified, or if the specified value is not a valid
	 * <code>boolean</code>, then <code>defaultValue</code> is returned.
	 * 
	 * @param name property name.
	 * @param defaultValue default value.
	 * @return property value as a <code>boolean</code>, 
	 *         or <code>defaultValue</code>. 
	 */
	public boolean getBoolean(String name, boolean defaultValue) {
		String valueString = get(name);
		if ("true".equals(valueString))
			return true;
		else if ("false".equals(valueString))
			return false;
		else
			return defaultValue;
	}
	
	/**
	 * Load a class by name.
	 * 
	 * @param name the class name.
	 * @return the class object.
	 * @throws ClassNotFoundException if the class is not found.
	 */
	public Class<?> getClassByName(String name) throws ClassNotFoundException {
		return Class.forName(name, true, classLoader);
	}
	
	/** 
	 * Get the value of the <code>name</code> property as a <code>Class</code>.  
	 * If no such property is specified, then <code>defaultValue</code> is 
	 * returned.
	 * 
	 * @param name the class name.
	 * @param defaultValue default value.
	 * @return property value as a <code>Class</code>, 
	 *         or <code>defaultValue</code>. 
	 */
	public Class<?> getClass(String name, Class<?> defaultValue) {
		String valueString = get(name);
		if (valueString == null)
			return defaultValue;
		try {
			return getClassByName(valueString);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}


	private synchronized Properties getProps() {
	
		List<Element> list = null;
		
		if (properties == null) {
			properties = new Properties();
			list = loadResources(resources, quietmode);
		
			Iterator<Element> iter = list.iterator();
			while(iter.hasNext()) {
				Element prop = iter.next();
				if (!"property".equals(prop.getTagName())) {
				//if (!"monitor_item".equals(prop.getTagName()))
					// LOG.warn("bad conf file: element not <property>");
					continue;
				}
				
				NodeList fields = prop.getChildNodes();
				String name = null;
				String value = null;
				for (int i = 0; i < fields.getLength(); i++) {
					Node fieldNode = fields.item(i);
					if (!(fieldNode instanceof Element))
						continue;
					Element field = (Element)fieldNode;
					if (propertyNameTagString.equals(field.getTagName()) && field.hasChildNodes())
						name = ((Text)field.getFirstChild()).getData().trim();
					if (propertyValueTagString.equals(field.getTagName()) && field.hasChildNodes())
						value = ((Text)field.getFirstChild()).getData();
				}
				
				if (name != null && value != null) {
					properties.setProperty(name, value);
				}
			}
		}
		
		return properties;
	}
	
	protected List<Element> loadResources(List<Object> resources, boolean quiet) {
		LinkedList<Element> list = new LinkedList<Element>();
		for (Object resource : resources) {
			list.addAll(loadResource(resource, quiet));
		}
		return list;
	}
	
	/**
	 * Get the {@link URL} for the named resource.
	 * 
	 * @param name
	 *            resource name.
	 * @return the url for the named resource.
	 */
	public URL locateResource(String name) {
		return classLoader.getResource(name);
	}

	protected List<Element> loadResource(Object name, boolean quiet) {
		
		LinkedList<Element> list = new LinkedList<Element>();
		
		try {
			DocumentBuilderFactory docBuilderFactory =
				DocumentBuilderFactory.newInstance();
			//ignore all comments inside the xml file
			docBuilderFactory.setIgnoringComments(true);
			DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
			Document doc = null;
			
			if (name instanceof URL) {                  // an URL resource
				URL url = (URL)name;
				if (url != null) {
					if (!quiet) {
						LOG.info("parsing " + url);
					}
					doc = builder.parse(url.toString());
				}
			} else if (name instanceof String) {        // a file resource
				
				/*
				File file = new File((String)name);
				if (file.exists()) {
					if (!quiet) {
						LOG.info("parsing " + file);
					}
					InputStream in = new BufferedInputStream(new FileInputStream(file));
					try {
						doc = builder.parse(in);
					} finally {
						in.close();
					}
				}
				*/
				
				/*
				 * TODO: used in released build
				 */ 
				URL url = locateResource((String) name);
				if (url != null) {
					if (!quiet) {
						LOG.info("parsing " + url);
					}
					doc = builder.parse(url.toString());
				}
			
			} else if (name instanceof File) {
				
				File file = (File) name;
				if (file.exists()) {
					if (!quiet) {
						LOG.info("parsing " + file);
					}
					InputStream in = new BufferedInputStream(new FileInputStream(file));
					try {
						doc = builder.parse(in);
					} finally {
						in.close();
					}
				}
				
			} else if (name instanceof InputStream) {
				try {
					doc = builder.parse((InputStream)name);
				} finally {
					((InputStream)name).close();
				}
			} else {
				LOG.debug("here");
			}

			if (doc == null) {
				if (quiet)
					return null;
				throw new RuntimeException(name + " not found");
			}
			
			Element root = doc.getDocumentElement();
			if (!"configuration".equals(root.getTagName()))
				LOG.fatal("bad conf file: top-level element not <configuration>");
			NodeList props = root.getChildNodes();
			for (int i = 0; i < props.getLength(); i++) {
				Node propNode = props.item(i);
				if (!(propNode instanceof Element))
					continue;
				Element prop = (Element)propNode;
				list.add(prop);
			}
		
		} catch (IOException e) {
			LOG.fatal("error parsing conf file: " + e);
			throw new RuntimeException(e);
		} catch (SAXException e) {
			LOG.fatal("error parsing conf file: " + e);
			throw new RuntimeException(e);
		} catch (ParserConfigurationException e) {
			LOG.fatal("error parsing conf file: " + e);
			throw new RuntimeException(e);
		}
		
		return list;
	}
	
	
	/**
	 * Get an {@link Iterator} to go through the list of <code>String</code>
	 * key-value pairs in the configuration.
	 * 
	 * @return an iterator over the entries.
	 */
	@Override
	public Iterator<Entry<String, String>> iterator() {
		Map<String, String> result = new HashMap<String, String>();
		for (Map.Entry<Object, Object> item : getProps().entrySet()) {
			if (item.getKey() instanceof String
					&& item.getValue() instanceof String) {
				result.put((String) item.getKey(), (String) item.getValue());
			}
		}
		return result.entrySet().iterator();
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print(System.getenv());
		Configuration conf = new Configuration(true);
		conf.setPropertyValueTagString("classpath");
		conf.getProps().list(System.out);
	}
	
}
