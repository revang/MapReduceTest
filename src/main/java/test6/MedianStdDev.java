package test6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import test2.MRDPUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MedianStdDev {
    public static class MedianStdDevMapper extends Mapper<Object, Text, IntWritable,IntWritable>{
        private IntWritable outHour=new IntWritable();
        private IntWritable outCommentLength=new IntWritable();
        private final static SimpleDateFormat frmt=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

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
            outCommentLength.set(text.length());

            context.write(outHour,outCommentLength);
        }
    }

    public static class MedianStdDevReducer extends Reducer<IntWritable,IntWritable,IntWritable,MedianStdDevTuple>{
        private MedianStdDevTuple result=new MedianStdDevTuple();
        private ArrayList<Float> commentLengths=new ArrayList<Float>();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float sum=0;
            float count=0;
            commentLengths.clear();
            result.setStdDev(0);

            for(IntWritable val:values){
                commentLengths.add((float)val.get());
                sum+=val.get();
                ++count;
            }

            Collections.sort(commentLengths);

            if(count%2==0){
                result.setMedian((commentLengths.get((int)count/2-1)+commentLengths.get((int)count/2))/2.0f);
            }else{
                result.setMedian(commentLengths.get((int)count/2));
            }

            float mean=sum/count;
            float sumOfSquares=0.0f;
            for(float f:commentLengths){
                sumOfSquares+=(f-mean)*(f-mean);
            }
            result.setStdDev((float)Math.sqrt(sumOfSquares/(count-1)));

            context.write(key,result);
        }
    }

    // 主程序
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.println("Usage: MedianStdDev <in><out>");
            System.exit(2);
        }

        Job job=new Job(conf,"MedianStdDev");
        job.setJarByClass(MedianStdDev.class);
        job.setMapperClass(MedianStdDevMapper.class);
        // 注意：这个案例中不能用combiner
        // job.setCombinerClass(MedianStdDevReducer.class);
        job.setReducerClass(MedianStdDevReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        // 注意：这里是job.setOutputValueClass(IntWritable.class); 不是job.MedianStdDevTuple(IntWritable.class);
        //      按照我的理解：有Combiner，设置Combiner的输出值；没有Combiner，设置Mapper的输出值
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
