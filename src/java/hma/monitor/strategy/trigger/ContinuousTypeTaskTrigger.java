/**
 * 
 */
package hma.monitor.strategy.trigger;

import hma.monitor.strategy.MonitorStrategyDataCollection;
import hma.monitor.strategy.MonitorStrategyWorker;

import java.util.Date;
import java.util.NavigableMap;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * @author guoyezhi
 *
 */
public class ContinuousTypeTaskTrigger implements TaskTrigger {
	
	private int threshold = 0;
	
	public ContinuousTypeTaskTrigger(Element conf) {
		
		NodeList subFieldNodes = conf.getChildNodes();
		
		String str = null;
		for (int i = 0; i < subFieldNodes.getLength(); i++) {
			Node subFieldNode = subFieldNodes.item(i);
			if (!(subFieldNode instanceof Element))
				continue;
			Element subField = (Element)subFieldNode;
			if ("continuity_threshold".equals(subField.getTagName())
					&& subField.hasChildNodes()) {
				str = ((Text)subField.getFirstChild()).getData().trim();
			}
		}
		if (str != null)
			this.threshold = Integer.parseInt(str);
	}

	/* (non-Javadoc)
	 * @see hma.monitor.strategy.trigger.TaskTrigger#visitMonitorStrategyWorker(hma.monitor.strategy.MonitorStrategyWorker)
	 */
	@Override
	public boolean visitMonitorStrategyWorker(MonitorStrategyWorker worker) {
		
		if (threshold < 1) {
			throw new RuntimeException();
		}
		
		NavigableMap<Date, MonitorStrategyDataCollection> dataProcessingCentre =
			worker.getStrategy().getDataProcessingCentre();
		Date timestamp = worker.getDataAcquisitionTimepoint();
		
		NavigableMap<Date, MonitorStrategyDataCollection> view =
			dataProcessingCentre.headMap(timestamp, true);
		System.err.println("\tview.size() = " + view.size());
		if (view.size() < threshold)
			return false;
		
		MonitorStrategyDataCollection dc = view.get(timestamp);
		for (int i = threshold; dc != null && i > 0; i--) {
			/*
			System.err.println("\t" + new Date(
					dc.get("NameNode_ListHDFSRootPath").getTimestamp()));
			*/
			if (dc.isAnomalous() == false) {
				System.err.println("\ti = " + i + ", dc.isAnomalous() == false");
				return false;
			}
			timestamp = view.lowerKey(timestamp);
			if (timestamp != null) {
				dc = view.get(timestamp);
			}
		}
		
		/*
		System.err.println("\tview.size() = " + view.size());
		Iterator<MonitorStrategyDataCollection> iter = view.values().iterator();
		System.err.println("\tview.firstKey() = " + view.firstKey());
		for (int i = threshold; iter.hasNext() && i > 0; i--) {
			MonitorStrategyDataCollection dc = iter.next();
			System.err.println("\t" + new Date(
					dc.get("NameNode_ListHDFSRootPath").getTimestamp()));
			System.err.println("\t" + dc.get("NameNode_ListHDFSRootPath").getData());
			if (dc.isAnomalous() == false) {
				System.err.println("\ti = " + i + ", dc.isAnomalous() == false");
				return false;
			}
		}
		*/
		
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}
