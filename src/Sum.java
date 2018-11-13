import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class Sum {
    //统计所有key的value的和
    public static class SumMapper extends Mapper<Text,Text, Text, IntWritable> {
        //输入为key及其数量，输出为key，及其转化为IntWritable的value
        @Override
        protected void map (Text key, Text value, Mapper<Text,Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // default InputFormat: KeyValueTextInputFormat
            IntWritable v=new IntWritable(Integer.parseInt(value.toString()));
            context.write(new Text("0"),v);
        }
    }

    public static class SumPartitioner extends Partitioner<Text, IntWritable> {
        //sum任务的reducer只能为1
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            return 0;
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
        @Override
        protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            Iterator<IntWritable> value=values.iterator();
            int valueCount=0;
            while(value.hasNext()){
                valueCount+=value.next().get();
            }
            context.write(new Text("value of sum:"),new IntWritable(valueCount));
        }
    }
}
