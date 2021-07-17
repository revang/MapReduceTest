package test7;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import test2.MRDPUtils;
import test6.MedianStdDevTuple;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

public class MedianStdDev {
    public static class MedianStdDevMapper extends Mapper<Object, Text, IntWritable,SortedMapWritable>{
        private IntWritable commentLength=new IntWritable();
        private static final LongWritable ONE=new LongWritable(1);
        private IntWritable outHour=new IntWritable();

        private static final SimpleDateFormat frmt=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String,String> parsed= MRDPUtils.transformXmlToMap(value.toString());

            String strDate=parsed.get("CreationDate");
            String text=parsed.get("Text");

            Date creationDate=new Date();
            try {
                creationDate=frmt.parse(strDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            outHour.set(creationDate.getHours());

            commentLength.set(text.length());
            SortedMapWritable outCommentLength=new SortedMapWritable();

            // 没有put方法：outCommentLength.put(commentLength,ONE);
            outCommentLength.setCommentLength(text.length());
            outCommentLength.setCount(1);

            context.write(outHour,outCommentLength);
        }
    }

    public static class MedianStdDevReducer extends Reducer<IntWritable,SortedMapWritable,IntWritable,MedianStdDevTuple>{
        private MedianStdDevTuple result=new MedianStdDevTuple();
        private TreeMap<Integer,Long> commentLengthCounts=new TreeMap<Integer, Long>();

        @Override
        protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
            float sum=0;
            long totalComments=0;
            commentLengthCounts.clear();
            result.setMedian(0);
            result.setStdDev(0);

            for(SortedMapWritable v:values){
                /*for(Map.Entry<WritableComparable, Writable> entry:v.entrySet()){

                }*/
            }
        }
    }
}
