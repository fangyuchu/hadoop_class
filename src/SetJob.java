import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class SetJob {
    public static Job setJob(Configuration conf,
                              String jobName,
                              Class<?> cls,
                              Class<? extends InputFormat> inputFormat,
                              Class<? extends Mapper> mapperClass,
                              Class<? extends Reducer> reducerClass,
                              int numtasks,
                              Class<?> mapOutputKeyClass,
                              Class<?> mapOutputValueClass,
                              Class<?> OutputKeyClass,
                              Class<?> OutputValueClass,
                              String inputPath,
                              String outputPath)throws Exception{
        Job j=Job.getInstance(conf, jobName);

        j.setJarByClass(cls);
        j.setInputFormatClass(inputFormat);

        j.setMapperClass(mapperClass);
        j.setReducerClass(reducerClass);
        j.setNumReduceTasks(numtasks);

        j.setMapOutputKeyClass(mapOutputKeyClass);
        j.setMapOutputValueClass(mapOutputValueClass);

        j.setOutputKeyClass(mapOutputKeyClass);
        j.setOutputKeyClass(mapOutputValueClass);

        FileInputFormat.addInputPath(j,new Path(inputPath));
        FileOutputFormat.setOutputPath(j,new Path(outputPath));
        return j;
    }
    public static Job setJob(Configuration conf,
                             String jobName,
                             Class<?> cls,
                             Class<? extends InputFormat> inputFormat,
                             Class<? extends Mapper> mapperClass,
                             Class<? extends Partitioner> partitionerClass,
                             Class<? extends Reducer> reducerClass,
                             int numtasks,
                             Class<?> mapOutputKeyClass,
                             Class<?> mapOutputValueClass,
                             Class<?> OutputKeyClass,
                             Class<?> OutputValueClass,
                             String inputPath,
                             String outputPath)throws Exception{
        Job j=setJob(conf,jobName,cls,inputFormat,mapperClass,reducerClass,numtasks,mapOutputKeyClass,mapOutputValueClass,
                mapOutputKeyClass,mapOutputValueClass,inputPath,outputPath);
        j.setPartitionerClass(partitionerClass);
        return j;
    }

}
