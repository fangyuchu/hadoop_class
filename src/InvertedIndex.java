import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
/**
 * Created with IntelliJ IDEA.
 * User: t4thswm
 * Date: 28/04/2017
 * Time: 10:38 AM
 */
public class InvertedIndex {
    //实现文档倒排索引。采用的key为<词，书>,value为1。自定义combiner和partitioner
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map (LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // default RecordReader: LineRecordReader; key: line offset; value: line string
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fName=fileSplit.getPath().getName();
            Text fileName = new Text(fName.substring(0,fName.length()-14));
            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());
            for (; itr.hasMoreTokens(); ) {
                word.set(itr.nextToken()+","+fileName);
                context.write(word,new IntWritable(1));                                                            //对应key和value
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            Iterator<IntWritable> it=values.iterator();
            int num=0;
            while(it.hasNext()){
                num++;
                it.next();
            }
            context.write(key,new IntWritable(num));
        }
    }

    public static class InvertedIndexPartitioner extends HashPartitioner<Text,IntWritable>{
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            String t = key.toString().split(",")[0];                                                                //<term, docid>=>term
            Text term=new Text(t);
            return super.getPartition(term,value,numReduceTasks);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
        String lastTerm=null;
        float termCount=0;                                                                                          //当前词出现的次数
        float bookCount=1;                                                                                          //当前词在多少本书中出现
        StringBuilder termInfo = new StringBuilder();                                                               //输出的字符串
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String term=key.toString().split(",")[0];                                                             //当前key是哪个词
            String termBook=key.toString().split(",")[1];                                                         //词在哪本书中出现
            int termBookNum=values.iterator().next().get();                                                             //在该书中出现的次数
            if(lastTerm==null) {
                lastTerm=term;
            }
            if(term.equals(lastTerm)){                                                                                  //是同一个词
                termCount+=termBookNum;
                termInfo.append(termBook+":"+termBookNum+";");
                bookCount++;
            }else {                                                                                                     //是一个新词
                termInfo.insert(0," "+(termCount/bookCount)+",");
                context.write(new Text(lastTerm),new Text(termInfo.toString()));                                        //将上一个词的信息输出
                lastTerm=term;
                bookCount=1;
                termCount=termBookNum;
                termInfo=new StringBuilder(termBook+":"+termBookNum+";");
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: InvertedIndex <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setPartitionerClass(InvertedIndexPartitioner.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setNumReduceTasks(3);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}