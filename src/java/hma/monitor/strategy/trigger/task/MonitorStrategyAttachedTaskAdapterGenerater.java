/**
 * 
 */
package hma.monitor.strategy.trigger.task;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * @author guoyezhi
 *
 */
public class MonitorStrategyAttachedTaskAdapterGenerater {
	
	@SuppressWarnings("unchecked")
	public static MonitorStrategyAttachedTaskAdapter 
	generateMonitorStrategyAttachedTaskAdapter(Element strategy) {
		
		String typeConf = null;
		String limitConf = null;
		String workerConf = null;
		String argsConf = null;
		
		//added by jiangtao
		String intervalConf = null;
		
		NodeList nodes = strategy.getChildNodes();
		for (int i = 0; i < nodes.getLength(); i++) {
			Node node = nodes.item(i);
			if (!(node instanceof Element))
				continue;
			Element e = (Element) node;
			if ("type".equals(e.getTagName()) && e.hasChildNodes())
				typeConf = ((Text)e.getFirstChild()).getData().trim();
			if ("limit".equals(e.getTagName()) && e.hasChildNodes()) {
				if (e.getFirstChild() == null) {
					System.err.println("e.getFirstChild() == null");
				} else if (((Text)e.getFirstChild()).getData() == null) {
					System.err.println("((Text)e.getFirstChild()).getData() == null");
				}
				limitConf = ((Text)e.getFirstChild()).getData().trim();
			}
			if ("interval".equals(e.getTagName()) && e.hasChildNodes()) {
                if (e.getFirstChild() == null) {
                    System.err.println("e.getFirstChild() == null");
                } else if (((Text)e.getFirstChild()).getData() == null) {
                    System.err.println("((Text)e.getFirstChild()).getData() == null");
                }
                intervalConf = ((Text)e.getFirstChild()).getData().trim();
            }
			if ("worker".equals(e.getTagName()) && e.hasChildNodes())
				workerConf = ((Text)e.getFirstChild()).getData().trim();
			if ("args".equals(e.getTagName()) && e.hasChildNodes())
				argsConf = ((Text)e.getFirstChild()).getData().trim();
		}
		
		Class<MonitorStrategyAttachedTask> taskType = null;
		if (typeConf != null) {
			try {
				taskType = 
					(Class<MonitorStrategyAttachedTask>) Class.forName(
						"hma.monitor.strategy.trigger.task.Attached"
						+ typeConf);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException();
			}
		}
		
		int continuousLimit = -1;
		int intervalLimit = -1;
		if (limitConf != null) {
			continuousLimit = Integer.parseInt(limitConf);
		}
		if (intervalConf != null) {
		    intervalLimit = Integer.parseInt(intervalConf);
		}
		
		if (taskType != null && (continuousLimit >= 0) && workerConf != null) {
            System.out.println("XXX typeConf = " + typeConf);
            System.out.println("XXX limitConf = " + limitConf);
            System.out.println("XXX workerConf = " + workerConf);
            System.out.println("XXX argsConf = " + argsConf);
			return new MonitorStrategyAttachedTaskAdapter(
					taskType, continuousLimit, workerConf, argsConf, intervalLimit);
		}
		
		System.out.println("XXX typeConf = " + typeConf);
		System.out.println("XXX limitConf = " + limitConf);
		System.out.println("XXX workerConf = " + workerConf);
        System.out.println("XXX argsConf = " + argsConf);
		
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
