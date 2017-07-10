package BinningPattern;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 * 
 */

public class PrimaryTypeDataMapper extends Mapper<Object, Text, Text, NullWritable> {
	
	private MultipleOutputs<Text, NullWritable> multipleOutputs;

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multipleOutputs.close();
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] col = value.toString().split(",");
		String primaryType = col[5].trim();
		
		if (primaryType != null) {
			multipleOutputs.write("bins", value, NullWritable.get(), primaryType);
		}
		
		
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multipleOutputs = new MultipleOutputs<>(context);
	}

}
