package test6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedianStdDevTuple implements Writable {
    private float stdDev=0;
    private float median=0;

    public float getStdDev() {
        return stdDev;
    }

    public void setStdDev(float stdDev) {
        this.stdDev = stdDev;
    }

    public float getMedian() {
        return median;
    }

    public void setMedian(float median) {
        this.median = median;
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(stdDev);
        out.writeFloat(median);
    }

    public void readFields(DataInput in) throws IOException {
        stdDev=in.readFloat();
        median=in.readFloat();
    }

    @Override
    public String toString() {
        return stdDev+"\t"+median;
    }
}
