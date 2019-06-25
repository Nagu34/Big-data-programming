

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriend {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if (line.length == 2) {
                
                String friend1 = line[0];
                List<String> values = Arrays.asList(line[1].split(","));
             
                for (String friend2 : values) {
                    int f1 = Integer.parseInt(friend1);
                    int f2 = Integer.parseInt(friend2);
                    if (f1 < f2)
                        word.set(friend1 + "," + friend2);
                    else
                        word.set(friend2 + "," + friend1);
                    context.write(word, new Text(line[1]));
                }
            }
        }

    }

public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            StringBuilder sb = new StringBuilder();
            for (Text friends : values) {
                List<String> temp = Arrays.asList(friends.toString().split(","));
                for (String friend : temp) {
                    if (map.containsKey(friend))
                        sb.append(friend + ','); 
                    else
                        map.put(friend, 1);

                }
            }
            if (sb.lastIndexOf(",") > -1) {
                sb.deleteCharAt(sb.lastIndexOf(","));
            }

            result.set(new Text(sb.toString()));
            context.write(key, result);
}

}       
public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.set("mapreduce.output.textoutputformat.separator", ",");

        if (otherArgs.length != 2) {
            System.err.println("Usage: matrix multiplication <input file> <output file>");
            System.exit(2);
        }

        

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "MutualFriend");
        job.setJarByClass(MutualFriend.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

