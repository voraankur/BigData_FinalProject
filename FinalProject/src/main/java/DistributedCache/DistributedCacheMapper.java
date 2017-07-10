package DistributedCache;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.common.hash.Funnel;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Sink;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

public class DistributedCacheMapper extends Mapper<Object, Text, Text, NullWritable> {

    Funnel<Crime> p = new Funnel<Crime>() {

		@Override
		public void funnel(Crime crime, Sink arg1) {
			// TODO Auto-generated method stub
			 arg1.putString(crime.crimeType, Charsets.UTF_8);
		}
    };

    private BloomFilter<Crime> crimes = BloomFilter.create(p, 500, 0.01);

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        String type_Crime;
        try {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if (files != null && files.length > 0) {

                for (Path file : files) {

                    try {
                        File myFile = new File(file.toUri());
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(myFile.toString()));
                        String line = null;
                        while ((line = bufferedReader.readLine()) != null) {
                            String[] split = line.split("\t");

                            type_Crime = String.valueOf(split[0]);
                            
                            Crime p = new Crime(type_Crime);
                            crimes.put(p);
                        }
                    } catch (IOException ex) {
                        System.err.println("Exception while reading  file: " + ex.getMessage());
                    }
                }
            }
        } catch (IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }

    }
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String values[] = value.toString().split(",");
        Crime c = new Crime( values[6]);
        if (crimes.mightContain(c)) {
            context.write(value, NullWritable.get());
        }

    }
}