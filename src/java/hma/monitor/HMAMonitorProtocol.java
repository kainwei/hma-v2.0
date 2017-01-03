package hma.monitor;

import org.apache.hadoop.ipc.VersionedProtocol;
/**
 * Created with IntelliJ IDEA.
 * User: uuleon
 * Date: 13-4-28
 * Time: 下午12:53
 * To change this template use File | Settings | File Templates.
 */
public interface HMAMonitorProtocol  extends VersionedProtocol {

    public boolean suspendAlarm(String cluster);

    public boolean recoverAlarm(String cluster);

    public MonitorStatus getAlarmStatus(String cluster);
}
