package DistributedCache;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistributedCacheMain {

	public static void main(String[] args) throws Exception {
		try {
	        Configuration conf = new Configuration();
	
	        Job job = Job.getInstance(conf, "Bloom Filter");
	        job.setJarByClass(DistributedCacheMain.class);
	        job.setMapperClass(DistributedCacheMapper.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(NullWritable.class);
	
	        //adding the file in the cache having the Person class records
	        DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());
	        job.setNumReduceTasks(0);
	
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	        job.waitForCompletion(true);
		} catch (IOException | InterruptedException | ClassNotFoundException ex) {
	           System.out.println("Error in Main" + ex.getMessage());
	       }

    }
}
