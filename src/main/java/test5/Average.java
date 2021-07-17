package test5;

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
import java.util.Date;
import java.util.Map;

public class Average {
    // Mapper
    public static class AverageMapper extends Mapper<Object, Text, IntWritable,CountAverageTuple>{
        private IntWritable outHour=new IntWritable();
        private CountAverageTuple outCountAverage=new CountAverageTuple();
        private final static SimpleDateFormat frmt=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String,String> parsed= MRDPUtils.transformXmlToMap(value.toString());

            String strDate=parsed.get("CreationDate");
            String text=parsed.get("Text");

            Date creationDate=new Date();
            try {
                creationDate = frmt.parse(strDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            outHour.set(creationDate.getHours());
            outCountAverage.setCount(1);
            outCountAverage.setAverage(text.length());

            context.write(outHour,outCountAverage);
        }
    }

    // Reducer
    public static class AverageReducer extends Reducer<IntWritable,CountAverageTuple,IntWritable,CountAverageTuple>{
        private CountAverageTuple result=new CountAverageTuple();

        @Override
        protected void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context) throws IOException, InterruptedException {
            double sum=0;
            long count=0;

            for(CountAverageTuple val:values){
                sum+=val.getAverage()*val.getCount();
                count+=val.getCount();
            }

            result.setCount(count);
            result.setAverage(sum/count);
            context.write(key,result);
        }
    }

    // 主程序
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.println("Usage: Average <in><out>");
            System.exit(2);
        }

        Job job=new Job(conf,"Average");
        job.setJarByClass(Average.class);
        job.setMapperClass(AverageMapper.class);
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(CountAverageTuple.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
