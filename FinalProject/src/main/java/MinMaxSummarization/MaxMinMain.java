package MinMaxSummarization;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxMinMain {
	
	 public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	        // TODO code application logic here
	       try{ 
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "MR Summarization");
	        job.setJarByClass(MaxMinMain.class);
	        job.setMapperClass(MaxMinMapper.class);
	        job.setCombinerClass(MaxMinCombiner.class);
	        job.setReducerClass(MaxMinReducer.class);

	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(CustomWritable.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
	        FileInputFormat.addInputPath(job,new Path(args[0]));
	        FileOutputFormat.setOutputPath(job,new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 :1);
	    }
	  catch(Exception e)
	  {
		e.printStackTrace();  
	  }
	  }

}
