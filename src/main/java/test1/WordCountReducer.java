package test1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {
        int total=0;
        for(IntWritable v2:v3) {
            total+=v2.get();
        }
        context.write(k3, new IntWritable(total));
    }
}
