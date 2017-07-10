package CrimeType;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TheftMapper extends Mapper<Object,Text,Text,IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
    	if (value.toString().contains("Date")) {
    		return;
    	}
    	
        String[] split = value.toString().split(",");
        String crimeType = split[5];
    
        context.write(new Text((crimeType)), one);
       
    }
    
}
