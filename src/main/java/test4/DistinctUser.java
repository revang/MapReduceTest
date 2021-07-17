package test4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import test2.MRDPUtils;

import java.io.IOException;
import java.util.Map;

public class DistinctUser {
    public static class DistinctUserMapper extends Mapper<Object, Text,Text, NullWritable>{
        private Text outUserId=new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String,String> parsed= MRDPUtils.transformXmlToMap(value.toString());
            String userId=parsed.get("UserId");
            outUserId.set(userId);
            context.write(outUserId,NullWritable.get());
        }
    }

    public static class DistinctUserReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    // 主程序
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.println("Usage: Distinct User <in><out>");
            System.exit(2);
        }

        Job job=new Job(conf,"Test: Distinct User");
        job.setJarByClass(DistinctUser.class);
        job.setMapperClass(DistinctUserMapper.class);
        job.setCombinerClass(DistinctUserReducer.class);
        job.setReducerClass(DistinctUserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
