package SecondarySorting;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class LocationCrimeCountPartitioner extends Partitioner<CompositeKeyWritable, NullWritable>{

    @Override
    public int getPartition(CompositeKeyWritable key, NullWritable value, int numOfPartitions) {
        
        return (key.getLocation().hashCode() % numOfPartitions);
    }
}
