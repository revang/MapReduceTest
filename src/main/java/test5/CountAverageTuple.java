package test5;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountAverageTuple implements Writable {
    private long count=0;
    private double average=0;

    public long getCount() {
        return count;
    }

    public double getAverage() {
        return average;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public void readFields(DataInput in) throws IOException {
        count=in.readLong();
        average=in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeDouble(average);
    }

    @Override
    public String toString() {
        return count+"\t"+average;
    }
}
