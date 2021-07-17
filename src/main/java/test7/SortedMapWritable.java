package test7;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortedMapWritable implements Writable {
    private int commentLength=0;
    private long count=0;

    public int getCommentLength() {
        return commentLength;
    }

    public void setCommentLength(int commentLength) {
        this.commentLength = commentLength;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(commentLength);
        out.writeLong(count);
    }

    public void readFields(DataInput in) throws IOException {
        commentLength=in.readInt();
        count=in.readLong();
    }

    @Override
    public String toString() {
        return commentLength + "\t" + count;
    }
}
