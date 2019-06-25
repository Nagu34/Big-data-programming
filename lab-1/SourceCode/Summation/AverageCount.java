import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;


public class AverageCount {
  
  public static void main(String[] args) throws IOException,
  InterruptedException, ClassNotFoundException {
   
	  
	 
      // Create configuration
      Configuration conf = new Configuration(true);
       String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.set("mapreduce.output.textoutputformat.separator", ",");

        if (otherArgs.length != 2) {
            System.err.println("Usage: matrix multiplication <input file> <output file>");
            System.exit(2);
        }

    Job job = new Job(conf, "KeyCount");
    job.setJarByClass(AverageCount.class);
    job.setJarByClass(KeyMapper.class);
    job.setJarByClass(KeyReducer.class);

   

    job.setMapperClass(KeyMapper.class);
    job.setCombinerClass(KeyCombine.class);
    job.setReducerClass(KeyReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

  
    // Execute job
    int code = job.waitForCompletion(true) ? 0 : 1;
    System.exit(code);


    
  }

  
}
