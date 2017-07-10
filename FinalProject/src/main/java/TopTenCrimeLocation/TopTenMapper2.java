package TopTenCrimeLocation;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper2 extends Mapper<Object, Text, Text, Text> {
	 
	private TreeMap<Integer, Text> locationMap = new TreeMap<Integer, Text>();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] row = (value.toString()).split("\\t");
        Text location = new Text(row[0]);
        int numIncidents  = Integer.parseInt(row[1].trim());
        
        locationMap.put(numIncidents, location);
        
        if(locationMap.size()>10) {
			locationMap.remove(locationMap.firstKey());
		}
         
	}

	public void cleanup(Context context)
			throws IOException, InterruptedException {
		for(int k: locationMap.descendingMap().keySet()) {
			context.write(new Text(locationMap.get(k)), new Text(String.valueOf(k)));
		}
	}
	
	

}
