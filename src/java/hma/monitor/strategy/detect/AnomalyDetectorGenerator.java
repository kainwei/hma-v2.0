/**
 * 
 */
package hma.monitor.strategy.detect;

import hma.util.LOG;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * @author guoyezhi
 *
 */
public class AnomalyDetectorGenerator {
	
	@SuppressWarnings("unchecked")
	private static SingleCycleAnomalyDetector generateSingleCycleAnomalyDetector(
			String monitorItemName,
			String monitorDataTypeName,
			String monitorDataSubTypeName,
			Element strategy) {
		
		if (monitorItemName.equals("SingleCycle") == false) {
			// TODO:
		}
		
		SingleCycleAnomalyDetector singleCycleDetector = null;
		
		if (monitorDataSubTypeName.equals("SingleLong")) {
			
			String conditionConf = null;
			String thresholdConf = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("condition".equals(subField.getTagName()) && subField.hasChildNodes())
					conditionConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("threshold".equals(subField.getTagName()) && subField.hasChildNodes())
					thresholdConf = ((Text)subField.getFirstChild()).getData().trim();
			}
			
			if (conditionConf != null && thresholdConf != null) {
				
				LOG.warn("conditionConf = " + conditionConf);
				LOG.warn("thresholdConf = " + thresholdConf);
				
				long threshold = Long.parseLong(thresholdConf);
				
				Class<SingleCycleSingleLongAnomalyDetector> detectorClass = null;
				try {
					LOG.warn("hma.monitor.strategy.detect."
							+ monitorDataTypeName
							+ monitorDataSubTypeName
							+ conditionConf
							+ "AnomalyDetector");
					
					detectorClass =
						(Class<SingleCycleSingleLongAnomalyDetector>) Class.forName(
								"hma.monitor.strategy.detect."
								+ monitorDataTypeName
								+ monitorDataSubTypeName
								+ conditionConf
								+ "AnomalyDetector");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
				
				try {
					singleCycleDetector =
						detectorClass.getConstructor(
								String.class, Long.class).newInstance(
										monitorItemName, threshold);
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
			
		}
		else if (monitorDataSubTypeName.equals("SingleDouble")) {
			
			String conditionConf = null;
			String thresholdConf = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("condition".equals(subField.getTagName()) && subField.hasChildNodes())
					conditionConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("threshold".equals(subField.getTagName()) && subField.hasChildNodes())
					thresholdConf = ((Text)subField.getFirstChild()).getData().trim();
			}
			
			if (conditionConf != null && thresholdConf != null) {
				
				double threshold = Double.parseDouble(thresholdConf);
				
				Class<SingleCycleSingleDoubleAnomalyDetector> detectorClass = null;
				try {
					detectorClass =
						(Class<SingleCycleSingleDoubleAnomalyDetector>) Class.forName(
								"hma.monitor.strategy.detect."
								+ monitorDataTypeName
								+ monitorDataSubTypeName
								+ conditionConf
								+ "AnomalyDetector");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
				
				try {
					singleCycleDetector =
						detectorClass.getConstructor(
								String.class, Double.class).newInstance(
										monitorItemName, threshold);
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
			
		}
		else if (monitorDataSubTypeName.equals("SingleString")) {
			
			String conditionConf = null;
			String patternConf = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("condition".equals(subField.getTagName()) && subField.hasChildNodes())
					conditionConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("pattern".equals(subField.getTagName()) && subField.hasChildNodes())
					patternConf = ((Text)subField.getFirstChild()).getData().trim();
			}
			
			//if (conditionConf != null && patternConf != null) {
			if (conditionConf != null) {
				
				LOG.warn("conditionConf = " + conditionConf);
				LOG.warn("patternConf = " + patternConf);
				
				Class<SingleCycleSingleStringAnomalyDetector> detectorClass = null;
				try {
					LOG.warn("hma.monitor.strategy.detect."
							+ monitorDataTypeName
							+ monitorDataSubTypeName
							+ conditionConf
							+ "AnomalyDetector");
					
					detectorClass =
						(Class<SingleCycleSingleStringAnomalyDetector>) Class.forName(
								"hma.monitor.strategy.detect."
								+ monitorDataTypeName
								+ monitorDataSubTypeName
								+ conditionConf
								+ "AnomalyDetector");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
				
				try {
					singleCycleDetector =
						detectorClass.getConstructor(
								String.class, String.class).newInstance(
										monitorItemName, patternConf);
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
			
		}
		else if (monitorDataSubTypeName.equals("CustomedSingleString")) {
			
			//System.out.println("monitorItemName = " + monitorItemName);
			//System.out.println("monitorDataTypeName = " + monitorDataTypeName);
			//System.out.println("monitorDataSubTypeName = " + monitorDataSubTypeName);
			
			String patternConf = null;
			String detectorConf = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("pattern".equals(subField.getTagName()) && subField.hasChildNodes())
					patternConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("detector".equals(subField.getTagName()) && subField.hasChildNodes()) {
					detectorConf = ((Text)subField.getFirstChild()).getData().trim();
				}
			}
			
			LOG.warn("patternConf = " + patternConf);
			LOG.warn("detectorConf = " + detectorConf);
			
			if (detectorConf != null) {
				
				//long threshold = Long.parseLong(thresholdConf);
				
				Class<SingleCycleSingleStringAnomalyDetector> detectorClass = null;
				try {
					detectorClass =
						(Class<SingleCycleSingleStringAnomalyDetector>) Class.forName(
								detectorConf);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				
				if (detectorClass != null) {
					
					try {
						singleCycleDetector =
							detectorClass.getConstructor(
									String.class, String.class).newInstance(
											monitorItemName, patternConf);
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					
				} else {
					System.out.println("!!");
				}
				
			}
			
		}
		else if (monitorDataSubTypeName.equals("Compound")) {
			
			//System.out.println("monitorItemName = " + monitorItemName);
			//System.out.println("monitorDataTypeName = " + monitorDataTypeName);
			//System.out.println("monitorDataSubTypeName = " + monitorDataSubTypeName);
			
			String detectorConf = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("detector".equals(subField.getTagName()) && subField.hasChildNodes()) {
					detectorConf = ((Text)subField.getFirstChild()).getData().trim();
				}
			}
			
			//LOG.warn("detectorConf = " + detectorConf);
			
			if (detectorConf != null) {
				
				//long threshold = Long.parseLong(thresholdConf);
				
				Class<SingleCycleCompoundTypeAnomalyDetector> detectorClass = null;
				try {
					detectorClass =
						(Class<SingleCycleCompoundTypeAnomalyDetector>) Class.forName(
								detectorConf);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				
				if (detectorClass != null) {
					
					try {
						singleCycleDetector =
							detectorClass.getConstructor(
									String.class).newInstance(monitorItemName);
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					
				} else {
					System.out.println("!!");
				}
				
			}
			
		}
		else {
			throw new RuntimeException(
					"Unknown Single Cycle Anomaly Detector Sub-Type");
		}
		
		return singleCycleDetector;
	}
	
	@SuppressWarnings("unchecked")
	private static DoubleCyclesAnomalyDetector generateDoubleCyclesAnomalyDetector(
			String monitorItemName,
			String monitorDataTypeName,
			String monitorDataSubTypeName,
			Element strategy) {
		
		if (monitorItemName.equals("DoubleCycles") == false) {
			// TODO:
		}
		
		DoubleCyclesAnomalyDetector doubleCyclesDetector = null;
		
		if (monitorDataSubTypeName.equals("SingleLong")) {
			
			String conditionConf = null;
			String thresholdConf = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("condition".equals(subField.getTagName()) && subField.hasChildNodes())
					conditionConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("threshold".equals(subField.getTagName()) && subField.hasChildNodes())
					thresholdConf = ((Text)subField.getFirstChild()).getData().trim();
			}
			
			if (conditionConf != null && thresholdConf != null) {
				
				LOG.warn("conditionConf = " + conditionConf);
				LOG.warn("thresholdConf = " + thresholdConf);
				
				long threshold = Long.parseLong(thresholdConf);
				
				Class<DoubleCyclesSingleLongAnomalyDetector> detectorClass = null;
				try {
					LOG.warn("hma.monitor.strategy.detect."
							+ monitorDataTypeName
							+ monitorDataSubTypeName
							+ conditionConf
							+ "AnomalyDetector");
					
					detectorClass =
						(Class<DoubleCyclesSingleLongAnomalyDetector>) Class.forName(
								"hma.monitor.strategy.detect."
								+ monitorDataTypeName
								+ monitorDataSubTypeName
								+ conditionConf
								+ "AnomalyDetector");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
				
				try {
					doubleCyclesDetector =
						detectorClass.getConstructor(
								String.class, Long.class).newInstance(
										monitorItemName, threshold);
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
			
		}
		else if (monitorDataSubTypeName.equals("SingleDouble")) {
			
			String conditionConf = null;
			String thresholdConf = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("condition".equals(subField.getTagName()) && subField.hasChildNodes())
					conditionConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("threshold".equals(subField.getTagName()) && subField.hasChildNodes())
					thresholdConf = ((Text)subField.getFirstChild()).getData().trim();
			}
			
			if (conditionConf != null && thresholdConf != null) {
				
				LOG.warn("conditionConf = " + conditionConf);
				LOG.warn("thresholdConf = " + thresholdConf);
				
				double threshold = Double.parseDouble(thresholdConf);
				
				Class<DoubleCyclesSingleDoubleAnomalyDetector> detectorClass = null;
				try {
					LOG.warn("hma.monitor.strategy.detect."
							+ monitorDataTypeName
							+ monitorDataSubTypeName
							+ conditionConf
							+ "AnomalyDetector");
					
					detectorClass =
						(Class<DoubleCyclesSingleDoubleAnomalyDetector>) Class.forName(
								"hma.monitor.strategy.detect."
								+ monitorDataTypeName
								+ monitorDataSubTypeName
								+ conditionConf
								+ "AnomalyDetector");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
				
				try {
					doubleCyclesDetector =
						detectorClass.getConstructor(
								String.class, Double.class).newInstance(
										monitorItemName, threshold);
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
			
		}
		else if (monitorDataSubTypeName.equals("Compound")) {
			
			String detectorConf = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("detector".equals(subField.getTagName()) && subField.hasChildNodes()) {
					detectorConf = ((Text)subField.getFirstChild()).getData().trim();
				}
			}
			
			if (detectorConf != null) {
				
				//long threshold = Long.parseLong(thresholdConf);
				
				Class<DoubleCyclesCompoundTypeAnomalyDetector> detectorClass = null;
				try {
					detectorClass =
						(Class<DoubleCyclesCompoundTypeAnomalyDetector>) Class.forName(
								detectorConf);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				
				if (detectorClass != null) {
					
					try {
						doubleCyclesDetector =
							detectorClass.getConstructor(
									String.class).newInstance(monitorItemName);
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					
				} else {
					System.out.println("!!");
				}
				
			}
			
		}
		else {
			throw new RuntimeException(
					"Unknown Double Cycles Anomaly Detector Sub-Type");
		}
		
		return doubleCyclesDetector;
	}
	
	@SuppressWarnings("unchecked")
	private static MultipleCyclesMapTypeAnomalyDetector<?> 
	generateMultipleCyclesMapTypeAnomalyDetector(
			String monitorItemName,
			String monitorDataTypeName,
			String monitorDataSubTypeName,
			Element strategy) {
		
		MultipleCyclesMapTypeAnomalyDetector<?> multipleCyclesMapDetector = null;
		
		if (monitorDataSubTypeName.equals("LongMap")) {
			
			String conditionConf = null;
			String cyclesConf = null;
			String thresholdConf = "0";
			Element subStrategy = null;
			
			SingleCycleSingleLongAnomalyDetector singleLongDetector = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("condition".equals(subField.getTagName()) && subField.hasChildNodes())
					conditionConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("cycles".equals(subField.getTagName()) && subField.hasChildNodes())
					cyclesConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("threshold".equals(subField.getTagName()) && subField.hasChildNodes())
					thresholdConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("sub-strategy".equals(subField.getTagName()) && subField.hasChildNodes())
					subStrategy = subField;
			}
			
			if (conditionConf != null && cyclesConf != null
					&& thresholdConf != null && subStrategy != null) {
				
				LOG.warn("monitorItemName = " + monitorItemName);
				
				singleLongDetector = (SingleCycleSingleLongAnomalyDetector) generateSingleCycleAnomalyDetector(
						monitorItemName, "SingleCycle", "SingleLong", subStrategy);
				
				if (singleLongDetector != null) {
					Class<MultipleCyclesLongMapAnomalyDetector> detectorClass = null;
					try {
						detectorClass =
							(Class<MultipleCyclesLongMapAnomalyDetector>) Class.forName(
									"hma.monitor.strategy.detect."
									+ monitorDataTypeName
									+ monitorDataSubTypeName
									+ conditionConf
									+ "AnomalyDetector");
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					
					int cycles = Integer.parseInt(cyclesConf);
					int threshold = Integer.parseInt(thresholdConf);
					
					try {
						multipleCyclesMapDetector = 
							detectorClass.getConstructor(String.class, Integer.class,
									Integer.class, SingleCycleSingleLongAnomalyDetector.class).newInstance(
											monitorItemName, cycles, threshold,
											singleLongDetector);
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}
			}
			
		}
		else if (monitorDataSubTypeName.equals("DoubleMap")) {
			
			String conditionConf = null;
			String cyclesConf = null;
			String thresholdConf = "0";
			Element subStrategy = null;
			
			SingleCycleSingleDoubleAnomalyDetector singleDoubleDetector = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("condition".equals(subField.getTagName()) && subField.hasChildNodes())
					conditionConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("cycles".equals(subField.getTagName()) && subField.hasChildNodes())
					cyclesConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("threshold".equals(subField.getTagName()) && subField.hasChildNodes())
					thresholdConf = ((Text)subField.getFirstChild()).getData().trim();
				if ("sub-strategy".equals(subField.getTagName()) && subField.hasChildNodes())
					subStrategy = subField;
			}
			
			if (conditionConf != null && cyclesConf != null
					&& thresholdConf != null && subStrategy != null) {
				
				singleDoubleDetector = (SingleCycleSingleDoubleAnomalyDetector) generateSingleCycleAnomalyDetector(
						monitorItemName, "SingleCycle", "SingleDouble", subStrategy);
				
				if (singleDoubleDetector != null) {
					Class<MultipleCyclesDoubleMapAnomalyDetector> detectorClass = null;
					try {
						detectorClass =
							(Class<MultipleCyclesDoubleMapAnomalyDetector>) Class.forName(
									"hma.monitor.strategy.detect."
									+ monitorDataTypeName
									+ monitorDataSubTypeName
									+ conditionConf
									+ "AnomalyDetector");
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					
					int cycles = Integer.parseInt(cyclesConf);
					int threshold = Integer.parseInt(thresholdConf);
					
					try {
						multipleCyclesMapDetector = 
							detectorClass.getConstructor(String.class, Integer.class,
									Integer.class, SingleCycleSingleDoubleAnomalyDetector.class).newInstance(
											monitorItemName, cycles, threshold,
											singleDoubleDetector);
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}
			}
			
		}
		else if (monitorDataSubTypeName.equals("StringMap")) {
			
		}
		else {
			throw new RuntimeException(
					"Unknown Multiple Map Anomaly Detector Sub-Type");
		}
		
		return multipleCyclesMapDetector;
	}
	
	@SuppressWarnings("unchecked")
	private static MultipleCyclesAnomalyDetector generateMultipleCyclesAnomalyDetector(
			String monitorItemName,
			String monitorDataTypeName,
			String monitorDataSubTypeName,
			Element strategy) {
		
		MultipleCyclesAnomalyDetector multipleCyclesDetector = null;
		
		if (monitorDataSubTypeName.endsWith("Map") == true) {
			multipleCyclesDetector = 
				generateMultipleCyclesMapTypeAnomalyDetector(monitorItemName,
						monitorDataTypeName, monitorDataSubTypeName, strategy);
		}
		else if (monitorDataSubTypeName.equals("Compound")) {
			
			String cyclesConf = null;
			String detectorConf = null;
			
			NodeList subFieldNodes = strategy.getChildNodes();
			for (int i = 0; i < subFieldNodes.getLength(); i++) {
				Node subFieldNode = subFieldNodes.item(i);
				if (!(subFieldNode instanceof Element))
					continue;
				Element subField = (Element) subFieldNode;
				if ("cycles".equals(subField.getTagName()) && subField.hasChildNodes()) {
					cyclesConf = ((Text)subField.getFirstChild()).getData().trim();
				}
				if ("detector".equals(subField.getTagName()) && subField.hasChildNodes()) {
					detectorConf = ((Text)subField.getFirstChild()).getData().trim();
				}
			}
			
			if (cyclesConf != null && detectorConf != null) {
				
				Class<MultipleCyclesCompoundTypeAnomalyDetector> detectorClass = null;
				try {
					detectorClass =
						(Class<MultipleCyclesCompoundTypeAnomalyDetector>) Class.forName(
								detectorConf);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				
				if (detectorClass != null) {
					
					try {
						multipleCyclesDetector =
							detectorClass.getConstructor(
									String.class, int.class).newInstance(
											monitorItemName, new Integer(cyclesConf).intValue());
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					
				} else {
					System.out.println("!!");
				}
			}
			
		}
		else {
			throw new RuntimeException("Unknown Multiple Cycles Anomaly Detector Type");
		}
		
		return multipleCyclesDetector;
	}
	
	public static AnomalyDetector generate(
			String monitorItemName,
			String monitorDataTypeName,
			String monitorDataSubTypeName,
			Element strategy) {
		
		AnomalyDetector detector = null;
		
		if (monitorDataTypeName.equals("SingleCycle")) {
			detector = generateSingleCycleAnomalyDetector(monitorItemName,
					monitorDataTypeName, monitorDataSubTypeName, strategy);
		}
		else if (monitorDataTypeName.equals("DoubleCycles")) {
			detector = generateDoubleCyclesAnomalyDetector(monitorItemName,
					monitorDataTypeName, monitorDataSubTypeName, strategy);
		}
		else if (monitorDataTypeName.equals("MultipleCycles")) {
			detector = generateMultipleCyclesAnomalyDetector(monitorItemName,
					monitorDataTypeName, monitorDataSubTypeName, strategy);
		}
		else {
			throw new RuntimeException("Unknown Anomaly Detector Type");
		}
		
		return detector;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
