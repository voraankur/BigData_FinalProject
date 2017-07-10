package TopTenCrimeLocation;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenReducer2 extends Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Integer, Text> locationMap = new TreeMap<Integer, Text>();
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		/*for (Text val: values) {
			context.write(NullWritable.get(), val);
		}*/
	
		for (Text value: values) {
						
			String[] row = (value.toString()).split("\\t");
	        //Text location = new Text(row[0]);
	        int numIncidents  = Integer.parseInt(row[1].trim());
			
			locationMap.put(numIncidents, new Text(value));
			
			if(locationMap.size()>10) {
				locationMap.remove(locationMap.firstKey());
			}
		}
		
		for (Text t: locationMap.descendingMap().values()){
			context.write(NullWritable.get(), t);
		}
		
	}

}
