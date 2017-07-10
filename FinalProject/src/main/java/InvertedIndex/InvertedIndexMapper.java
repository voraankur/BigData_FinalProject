package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
    
    private Text area = new Text();
     private Text type = new Text();     
   public void map(Object key, Text values, Context context)
   {
        try {
            String[] tokens = values.toString().split(",");
            type.set(tokens[5]);
            area.set(tokens[11]);
            
            context.write(type,area);
        } catch (IOException | InterruptedException ex) {
            System.out.println("Erorr in Mapper"+ex.getMessage());
        }
               
   }
}
