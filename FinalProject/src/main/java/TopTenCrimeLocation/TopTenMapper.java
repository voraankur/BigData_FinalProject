package TopTenCrimeLocation;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper extends Mapper<Object, Text, Text, IntWritable> {

	
	private final static IntWritable one = new IntWritable(1);
		
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			
		String[] attr = value.toString().split(",");
		
		String location = attr[3];
			
		context.write(new Text((location)), one);
		
	}
	
}
