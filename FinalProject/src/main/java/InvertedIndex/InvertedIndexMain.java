package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndexMain {
	
	public static void main(String[] args) {
        try {
           Configuration conf = new Configuration();
           Job job = Job.getInstance(conf, "Inverted Index");
           job.setJarByClass(InvertedIndexMain.class);
           job.setMapperClass(InvertedIndexMapper.class);

           job.setMapOutputKeyClass(Text.class);
           job.setMapOutputValueClass(Text.class);

           job.setReducerClass(InvertedIndexReducer.class);
           job.setOutputKeyClass(Text.class);
           job.setOutputValueClass(Text.class);
           FileInputFormat.addInputPath(job, new Path(args[0]));
           TextOutputFormat.setOutputPath(job, new Path(args[1]));
           job.waitForCompletion(true);
       } catch (IOException | InterruptedException | ClassNotFoundException ex) {
           System.out.println("Error in Main" + ex.getMessage());
       }
   }

}
