package BinningPattern;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PrimaryTypeData {

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "Primary Type Data");
            job.setJarByClass(PrimaryTypeData.class);

            job.setMapperClass(PrimaryTypeDataMapper.class);

            job.setNumReduceTasks(0);

            MultipleOutputs.addNamedOutput(job, "bins",
                    TextOutputFormat.class,
                    Text.class,
                    NullWritable.class);

            MultipleOutputs.setCountersEnabled(job, true);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e) {
        	System.out.println(e);
        }

        
    }
}
