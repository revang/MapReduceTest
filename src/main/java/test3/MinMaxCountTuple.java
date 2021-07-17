package test3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MinMaxCountTuple implements Writable {
    private Date min=new Date();
    private Date max=new Date();
    private long count=0;

    private final static SimpleDateFormat fmt=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    // 实线get、set方法
    public Date getMin() {
        return min;
    }

    public Date getMax() {
        return max;
    }

    public long getCount() {
        return count;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public void setCount(long count) {
        this.count = count;
    }

    // 实线Writeable接口读方法
    public void readFields(DataInput in) throws IOException {
        min=new Date(in.readLong());
        max=new Date(in.readLong());
        count=in.readLong();
    }

    // 实线Writeable接口写方法
    public void write(DataOutput out) throws IOException {
        out.writeLong(min.getTime());
        out.writeLong(max.getTime());
        out.writeLong(count);
    }

    @Override
    public String toString() {
        return fmt.format(min)+"\t"+fmt.format(max)+"\t"+count;
    }
}
