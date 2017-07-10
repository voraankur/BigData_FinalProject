package SecondarySorting;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LocationCrimeCountReducer extends Reducer<CompositeKeyWritable, IntWritable, CompositeKeyWritable, IntWritable> {
	
	IntWritable result = new IntWritable();
	@Override
    public void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context)
    {
		int sum = 0;
    	try{
        for(IntWritable val:values)
        {
        	sum += 1;
            val.get();
        }
        result.set(sum);
        context.write(key, result);
        System.out.println(key.toString());
    	}
        catch(IOException | InterruptedException ex){
            System.err.println("Error Message" + ex.getMessage());
        }      
    }

}
