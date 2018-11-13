import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.net.URI;
import java.util.*;
public class SocialTriangle {
    public static class adJacentNodeDetectReducer extends Reducer<DoubleWritable, DoubleWritable,Text, IntWritable>{
        //输出和key节点相邻的节点,新生成的<key,value>为<"相邻节点1 id，相邻节点2 id"，1>(id小的在前)。同时，原有的<key,value>转化为<"key,value",1>输出
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
                context.write(new Text(key.toString()+","+node.toString()),new IntWritable(0));
            }
            Double iNode;
            Double jNode;
            for(int i=0;i<nodeList.size();i++){
                iNode=nodeList.get(i);
                for(int j=i+1;j<nodeList.size();j++){
                    jNode=nodeList.get(j);
                    if(iNode<jNode) {

                        context.write(new Text(iNode.toString()+","+jNode.toString()),new IntWritable(1));
                    }else {
                        context.write(new Text(jNode.toString()+","+iNode.toString()),new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class triangleCountMapper extends Mapper<Text,Text, Text, IntWritable> {
        //输入为key及其数量，输出为key，及其value转化为IntWritable的value
        @Override
        protected void map (Text key, Text value, Mapper<Text,Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // default InputFormat: KeyValueTextInputFormat
            IntWritable v=new IntWritable(Integer.parseInt(value.toString()));
            context.write(key,v);
        }
    }

    public static class triangleCountReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
        @Override
        protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            Iterator<IntWritable> value=values.iterator();
            int num=0;
            boolean edgeExist=false;
            while(value.hasNext()){
                int a=value.next().get();
                if(a==0)edgeExist=true;
                num++;
            }
            num--;
            if(edgeExist){
                context.write(key,new IntWritable(num));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        String tempOutputDir1="tempoutput1";
        String tempOutputDir2="tempoutput2";
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: jar_name <in> <out>");
            System.exit(2);
        }

        Job nd = SetJob.setJob(
                conf,"GraphConversion",SocialTriangle.class,
                TextInputFormat.class,
                GraphConversion.graphConversionMapper.class,
                GraphConversion.GraphConversionPartitioner.class,
                SocialTriangle.adJacentNodeDetectReducer.class,
                10,
                DoubleWritable.class,DoubleWritable.class,Text.class,IntWritable.class,
                args[0],tempOutputDir1);

        Job tc=SetJob.setJob(
                conf,"TriangleCount",SocialTriangle.class,
                KeyValueTextInputFormat.class,
                triangleCountMapper.class,
                triangleCountReducer.class,
                10,
                Text.class,IntWritable.class,Text.class,IntWritable.class,
                tempOutputDir1,tempOutputDir2);

        Job sm=SetJob.setJob(
                conf,"Sum",Sum.class,
                KeyValueTextInputFormat.class,
                Sum.SumMapper.class,
                Sum.SumPartitioner.class,
                Sum.SumReducer.class,
                1,
                Text.class,IntWritable.class,Text.class,IntWritable.class,
                tempOutputDir2,args[1]
        );
        sm.waitForCompletion(tc.waitForCompletion(nd.waitForCompletion(true)));                                 //执行任务
        FileSystem fs= FileSystem.get(URI.create(tempOutputDir1),conf);                                                 //删除临时文件
        fs.delete(new Path(tempOutputDir1),true);
        fs= FileSystem.get(URI.create(tempOutputDir2),conf);
        fs.delete(new Path(tempOutputDir2),true);
        System.exit(1);
    }
}

