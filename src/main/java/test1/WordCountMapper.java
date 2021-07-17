package test1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String data=v1.toString(); // data：I love Beijing
        String[] words=data.split(" ");
        for(String w:words) {
            context.write(new Text(w), new IntWritable(1));
        }
    }
}