package test2;

import org.apache.commons.text.StringEscapeUtils;
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

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class CommentWordCount {
    // Mapper
    public static class WordCountMapper extends Mapper<Object, Text,Text, IntWritable> {
        private final static IntWritable one=new IntWritable(1);
        private Text word=new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 将xml转换成map，获取Text对象
            Map<String,String> parsed=MRDPUtils.transformXmlToMap(value.toString());
            String txt=parsed.get("Text");
            if(txt==null){
                return;
            }

            // 数据清洗，将txt转换成小写，删除所有单引号，将非字符都替换成空格
            txt= StringEscapeUtils.unescapeHtml4(txt.toLowerCase());
            // txt=txt.toLowerCase();
            txt=txt.replaceAll("'","");
            txt=txt.replaceAll("[^a-zA-Z]"," ");

            // 根据txt生成迭代器
            StringTokenizer itr=new StringTokenizer(txt);

            // 循环迭代输出
            while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word,one);
            }

        }
    }

    // Reducer
    public static class IntSumReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
        private IntWritable result=new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable val:values){
                sum+=val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    // 主程序
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length!=2){
            System.err.println("Usage: CommentWordCount <in><out>");
            System.exit(2);
        }

        Job job=new Job(conf,"StackOverflow Comment Word Count");
        job.setJarByClass(CommentWordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
