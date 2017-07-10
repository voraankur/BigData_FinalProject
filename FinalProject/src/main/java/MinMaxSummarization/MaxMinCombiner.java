package MinMaxSummarization;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxMinCombiner extends Reducer<Text,CustomWritable,Text,Text>{
    
   
    public void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException
    {
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        String type_max = "";
        String type_min = "";
        try{
            for (CustomWritable val : values)
                {
                if(val.getCount() > max)
                    {
                        max = val.getCount();
                        type_max = "max: " + max + " --> " + val.getType() + ", " ;
                    }
                if(val.getCount() < min)
                    {
                        min = val.getCount();
                        type_min = "min: " + min + " --> " + val.getType();
                    }
                }
            if(!key.equals("")) {
            context.write(key,new Text(type_max + " " + type_min));
            }
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
    }
}
