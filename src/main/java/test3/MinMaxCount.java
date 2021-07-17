package test3;

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
import test2.CommentWordCount;
import test2.MRDPUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class MinMaxCount {
    public static class MinMaxCountMapper extends Mapper<Object, Text, Text,MinMaxCountTuple>{
        private Text outUserId=new Text();
        private MinMaxCountTuple outTuple=new MinMaxCountTuple();

        // 设置日期格式化样式
        // new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse("2011-08-04T09:30:25.043") ---> Thu Aug 04 09:30:25 CST 2011
        private final static SimpleDateFormat frmt=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 将XML转换成Map对象
            Map<String,String> parsed= MRDPUtils.transformXmlToMap(value.toString());

            // 获取userId、CreationDate
            String userId=parsed.get("UserId");
            String strDate=parsed.get("CreationDate");
            Date creationDate= null;
            try {
                creationDate = frmt.parse(strDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            // 设置userId
            outUserId.set(userId);

            // 设置最大值、最小值、计数器
            outTuple.setMax(creationDate);
            outTuple.setMax(creationDate);
            outTuple.setCount(1);

            // 输出
            context.write(outUserId,outTuple);
        }
    }

    public static class MinMaxCountReducer extends Reducer<Text,MinMaxCountTuple,Text, MinMaxCountTuple>{
        private MinMaxCountTuple result=new MinMaxCountTuple();

        @Override
        protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
            // 初始化结果
            // 注意：如果使用result.setMin(null)初始化在val.getMin().compareTo(result.getMin())<0会报Error: java.lang.NullPointerException
            // 注意：result.setMin(new Date(0))初始化会导致最小值不对
            // result.setMin(null);
            // result.setMax(null);
            result.setMin(new Date(0));
            result.setMin(new Date(0));
            result.setCount(0);
            int sum=0;

            // 循环遍历输入值
            for(MinMaxCountTuple val:values){
                // 设置最小值
                if(val.getMin()==null||val.getMin().compareTo(result.getMin())<0){
                    result.setMin(val.getMin());
                }

                // 设置最大值
                if(val.getMax()==null||val.getMax().compareTo(result.getMax())>0){
                    result.setMax(val.getMax());
                }

                // 设置计数器
                sum+=1;
            }

            // 计数器赋值
            result.setCount(sum);

            // 输出
            context.write(key,result);
        }
    }

    // 主程序
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.println("Usage: MinMaxCount <in><out>");
            System.exit(2);
        }

        Job job=new Job(conf,"MinMaxCount");
        job.setJarByClass(MinMaxCount.class);
        job.setMapperClass(MinMaxCountMapper.class);
        job.setCombinerClass(MinMaxCountReducer.class);
        job.setReducerClass(MinMaxCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxCountTuple.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
