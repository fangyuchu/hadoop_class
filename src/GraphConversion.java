import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.*;
public class GraphConversion {
    //实现有向图转化为无向图，key为id（小的），value为该id链接的其他id（都比key大）
    public static class graphConversionMapper extends Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {
        @Override
        protected void map (LongWritable key, Text value, Mapper<LongWritable, Text, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
            // default RecordReader: LineRecordReader; key: line offset; value: line string
            StringTokenizer itr = new StringTokenizer(value.toString());
            for (; itr.hasMoreTokens(); ) {
                double node1=Double.parseDouble(itr.nextToken());
                double node2=Double.parseDouble(itr.nextToken());
                if(node1<node2){
                    context.write(new DoubleWritable(node1),new DoubleWritable(node2));
                }else if (node2<node1){
                    context.write(new DoubleWritable(node2),new DoubleWritable(node1));
                }

            }
        }
    }

    public static class GraphConversionPartitioner extends Partitioner<DoubleWritable,DoubleWritable>{
        @Override
        public int getPartition(DoubleWritable key, DoubleWritable value, int numReduceTasks){
            return (int)(key.get())%numReduceTasks;
        }
    }

    public static class graphConversionReducer extends Reducer<DoubleWritable, DoubleWritable,DoubleWritable, DoubleWritable>{
        @Override
        protected void reduce(DoubleWritable key,Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException{
            Iterator<DoubleWritable> value=values.iterator();
            Map<Double,Boolean> nodeList=new HashMap<>();
            while(value.hasNext()){
                Double node=value.next().get();
                if(nodeList.containsKey(node)){
                    continue;
                }
                nodeList.put(node,true);
                context.write(key,new DoubleWritable(node));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: GraphConversion <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Graph Conversion");
        job.setJarByClass(GraphConversion.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(GraphConversion.graphConversionMapper.class);
        job.setReducerClass(GraphConversion.graphConversionReducer.class);

        job.setNumReduceTasks(3);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
