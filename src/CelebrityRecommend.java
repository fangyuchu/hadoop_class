import org.apache.commons.io.IOUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;
public class CelebrityRecommend {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //System.out.println(value.toString());
            String s=value.toString();
            String[] splits=s.split(" ");
            Text k=new Text(splits[0]);
            Text v=new Text(s.substring(splits[0].length()+1));

//            System.out.print(k+":   ");
//            System.out.println(v);
//
//            System.out.println("----------------------------------------------------------------------------------");

            context.write(k, v);
        }
    }


    public static class Reduce extends Reducer<Text, Text,Text, Text>{

        public ArrayList<Text> celebrity=new ArrayList<Text>();                                         //celebrities' id
        public ArrayList<ArrayList<Double>> celebVector=new ArrayList<ArrayList<Double>>();             //celebrities' vector

        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            try{  // 将distributed cache file装入各个Map节点本地的内存数据joinData中
                Path [] cacheFiles= context.getLocalCacheFiles();
                URI[] c=context.getCacheFiles();
                if (cacheFiles != null && cacheFiles.length> 0) {

                    BufferedReader br = new BufferedReader(new FileReader("cache"));
                    String s1 = null;
                    while ((s1 = br.readLine()) != null)
                    {
                        String[] splits= s1.split(" ");
                        celebrity.add(new Text(splits[0]));
                        ArrayList<Double> vec=new ArrayList<>();
                        for(int i=1;i<splits.length;i++){
                            vec.add(Double.parseDouble(splits[i]));
                        }
                        celebVector.add(vec);
                    }

//                    System.out.println("-----------------------------------------------------------------------------------------------------");
//                    System.out.println(celebrity);
//                    System.out.println(celebVector.get(0));
//                    System.out.println("-----------------------------------------------------------------------------------------------------");
                }
            }catch (IOException e) {
                System.err.println("Exception reading DistributedCache: "+ e);
            }
        }

        @Override
        protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
            Iterator<Text> value=values.iterator();
            while(value.hasNext()){
                String s=value.next().toString();
                String[] splits=s.split(" ");
                ArrayList<Double> vec=new ArrayList<>();
                for(int i=0;i<splits.length;i++){
                    vec.add(Double.parseDouble(splits[i]));
                }
                Text celebID=celebrity.get(0);
                Double minDistance=calDistance(vec,celebVector.get(0));
                for(int i=1;i<celebVector.size();i++){
                    Double distance=calDistance(vec,celebVector.get(i));
                    if(minDistance>distance){
                        minDistance=distance;
                        celebID=celebrity.get(i);
                    }
                }
                context.write(key,celebID);
            }
        }

        public Double calDistance(ArrayList<Double> a,ArrayList<Double> b){
            Double distance=0.0;
            for(int i=0;i<a.size();i++){
                distance+=Math.pow(a.get(i)-b.get(i),2);
            }
            return distance;
        }
    }

    public static void main(String[] args) throws Exception{
        String input="/input/CelebrityRecommend";
        String output="output";
        String distributedFileLoc="/input/cache/celebrity.txt";
        Configuration conf = new Configuration();
        FileSystem fs= FileSystem.get(URI.create(output),conf);                                                //删除临时文件
//        if(fs.exists(new Path(tempOutputDir1))) {
//            System.out.println("-----------------------------------------------------------------------------------------------------");
//            System.out.println("delete directory:"+tempOutputDir1);
//            System.out.println("-----------------------------------------------------------------------------------------------------");
//            fs.delete(new Path(tempOutputDir1),true);
//        }
//        if(fs.exists(new Path(tempOutputDir2))){
//            System.out.println("-----------------------------------------------------------------------------------------------------");
//            System.out.println("delete directory:"+tempOutputDir2);
//            System.out.println("-----------------------------------------------------------------------------------------------------");
//            fs= FileSystem.get(URI.create(tempOutputDir2),conf);
//            fs.delete(new Path(tempOutputDir2),true);
//        }
        if(fs.exists(new Path(output))){
            System.out.println("-----------------------------------------------------------------------------------------------------");
            System.out.println("delete directory:"+output);
            System.out.println("-----------------------------------------------------------------------------------------------------");
            fs= FileSystem.get(URI.create(output),conf);
            fs.delete(new Path(output),true);
        }

        Job j=Job.getInstance(conf, "celebrityRecommend");



//        j.addCacheFile(new Path(distributedFileLoc).toUri());
        j.addCacheFile(new URI(distributedFileLoc+"#cache"));




        j.setJarByClass(CelebrityRecommend.class);
        j.setInputFormatClass(TextInputFormat.class);

        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);
        j.setNumReduceTasks(3);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Text.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputKeyClass(Text.class);

        FileInputFormat.addInputPath(j,new Path(input));
        FileOutputFormat.setOutputPath(j,new Path(output));

        j.waitForCompletion(true);                                 //执行任务


        try {                                                                                                           //将最终计数结果读出并输出
            InputStream in = fs.open(new Path(output+"/part-r-00000"));
            System.out.println("-----------------------------------------------------------------------------------------------------");
            IOUtils.copy(in,System.out);
            System.out.println("-----------------------------------------------------------------------------------------------------");
        }catch (Exception e){
            e.printStackTrace();
        }
        fs= FileSystem.get(URI.create(output),conf);
        fs.delete(new Path(output),true);
        System.exit(1);
    }
}
