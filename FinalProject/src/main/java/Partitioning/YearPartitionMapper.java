package Partitioning;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YearPartitionMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(value.toString().contains("Date")) {
			return;
		}
		
		String[] col = value.toString().split(",");
		String year = col[17].trim();
		
		context.write(new Text(year), value);
			
	}

	
}
