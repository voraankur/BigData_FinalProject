package MinMaxSummarization;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxMinMapper extends Mapper<Object, Text, Text, CustomWritable>{
    
	Text type = new Text();
    public void map(Object key, Text value, Context context)
    {
    	 try{
    	 
    	if(value.toString().contains("Date"))
    	            return;
    	
        
    	String split[] = value.toString().split("\t");

    	CustomWritable cw = new CustomWritable();
    	cw.setCount(Integer.parseInt(split[2]));
    	cw.setType(split[1]);
    	     
        String location = split[0];
        context.write(new Text(location),cw);
            context.write(new Text(type),cw);
        }
        catch(IOException | InterruptedException ex){
        System.out.println("Error Message" + ex.getMessage());
        }
    }
}