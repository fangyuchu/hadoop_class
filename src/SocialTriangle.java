import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
public class SocialTriangle {
    public static class adJacentNodeDetectReducer extends Reducer<DoubleWritable, DoubleWritable,DoubleWritable, DoubleWritable>{
        //输出和key节点相邻的节点,以及这些节点之间两两配对生成的新《key，value》。即新的key,value为两个与原key相邻的节点，且新key的id小于新value
        @Override
        protected void reduce(DoubleWritable key,Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException{
            Iterator<DoubleWritable> value=values.iterator();
            if(!value.hasNext())return;                                                                                 //该key节点没有相连节点
            Vector<Double> nodeList=new Vector<>();
            while(value.hasNext()){
                Double node=value.next().get();
                if(nodeList.contains(node)){
                    continue;
                }
                nodeList.add(node);
                context.write(key,new DoubleWritable(node));
            }
            double iNode;
            double jNode;
            for(int i=0;i<nodeList.size();i++){
                iNode=nodeList.get(i);
                for(int j=i+1;j<nodeList.size();j++){
                    jNode=nodeList.get(j);
                    if(iNode<jNode) {
                        context.write(new DoubleWritable(iNode), new DoubleWritable(jNode));
                    }else {
                        context.write(new DoubleWritable(jNode), new DoubleWritable(iNode));
                    }
                }
            }
        }
    }

//    public static class triangleCount extends Mapper<DoubleWritable, DoubleWritable,DoubleWritable, DoubleWritable>{
//        @Override
//        protected void map (DoubleWritable key, DoubleWritable value, Mapper<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
//
//        }
//    }
    public static class triangleCountMapper extends Mapper<Text,Text, DoubleWritable, DoubleWritable> {
        @Override
        protected void map (Text key, Text value, Mapper<Text,Text, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
            // default RecordReader: LineRecordReader; key: line offset; value: line string
            DoubleWritable k=new DoubleWritable(Double.parseDouble(key.toString()));
            DoubleWritable v=new DoubleWritable(Double.parseDouble(value.toString()));
            context.write(k,v);
        }
    }
    public static class triangleCountReducer extends Reducer<DoubleWritable, DoubleWritable,DoubleWritable, DoubleWritable>{
        //输出和key节点相邻的节点对。即新输出的key,value为两个与原key相邻的节点，且新key的id小于新value
        @Override
        protected void reduce(DoubleWritable key,Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException{
            Iterator<DoubleWritable> value=values.iterator();
            double num=0;
            while(value.hasNext()){
                value.next();
                num++;
            }
            context.write(new DoubleWritable(num-1),new DoubleWritable(0));
        }
    }
    public static void main(String[] args) throws Exception{
        String tempOutputDir="/tempoutput";
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: SocialTriangle <in> <out>");
            System.exit(2);
        }

        Job nd = Job.getInstance(conf, "GraphConversion");
        nd.setJarByClass(SocialTriangle.class);
        nd.setInputFormatClass(TextInputFormat.class);

        nd.setMapperClass(GraphConversion.graphConversionMapper.class);
        nd.setReducerClass(SocialTriangle.adJacentNodeDetectReducer.class);
        nd.setNumReduceTasks(3);

        nd.setMapOutputKeyClass(DoubleWritable.class);
        nd.setMapOutputValueClass(DoubleWritable.class);

        nd.setOutputKeyClass(DoubleWritable.class);
        nd.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(nd, new Path(args[0]));
        FileOutputFormat.setOutputPath(nd, new Path(tempOutputDir));




        Job tc=Job.getInstance(conf, "TriangleCount");

        tc.waitForCompletion(nd.isComplete());                                                                          //等待上一个任务完成
        tc.setJarByClass(SocialTriangle.class);
        tc.setInputFormatClass(KeyValueTextInputFormat.class);

        tc.setMapperClass(SocialTriangle.triangleCountMapper.class);
        tc.setReducerClass(SocialTriangle.triangleCountReducer.class);
        tc.setNumReduceTasks(3);

        tc.setOutputKeyClass(DoubleWritable.class);
        tc.setOutputKeyClass(DoubleWritable.class);
        FileInputFormat.addInputPath(tc,new Path(tempOutputDir));
        FileOutputFormat.setOutputPath(tc,new Path(args[1]));

        System.exit(tc.waitForCompletion(true) ? 0 : 1);
    }
}

