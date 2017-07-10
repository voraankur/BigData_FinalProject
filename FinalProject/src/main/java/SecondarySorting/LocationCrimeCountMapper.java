package SecondarySorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocationCrimeCountMapper extends Mapper<Object, Text, CompositeKeyWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(Object key, Text value, Context context) {
	try{
		if(value.toString().contains("Date")) {
			return;
		}
		
		String[] cols = value.toString().split(",");
		String location = cols[7];
		String crimeType = cols[5];
		
		CompositeKeyWritable cw = new CompositeKeyWritable(location,crimeType);
		context.write(cw,one);
	}catch(IOException | InterruptedException ex){
        System.out.println("Error Message" + ex.getMessage());
        }	
	}
}
