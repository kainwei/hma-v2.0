package hma.monitor;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: uuleon
 * Date: 13-4-28
 * Time: 下午11:04
 * To change this template use File | Settings | File Templates.
 */
public class MonitorStatus implements Writable {

    private boolean isAlarm;
    private long begin, end;

    public MonitorStatus() {}

    public void setAlarm(boolean alarm){
        this.isAlarm = alarm;
    }

    public boolean getAlarm(){
        return isAlarm;
    }

    public void setBegin(long begin) {
        this.begin = begin;
    }

    public long getBegin(){ return begin;}

    public void setEnd(long end) { this.end = end;}

    public long getEnd() { return end;}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(isAlarm);
        out.writeLong(begin);
        out.writeLong(end);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.isAlarm = in.readBoolean();
        this.begin = in.readLong();
        this.end = in.readLong();
    }
}
