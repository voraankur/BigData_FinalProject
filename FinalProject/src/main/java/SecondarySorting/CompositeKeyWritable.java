package SecondarySorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

	private String location;
	private String crimeType;
	
	public CompositeKeyWritable() {
	}

	public CompositeKeyWritable(String location, String crimeType) {
		super();
		this.location = location;
		this.crimeType = crimeType;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getCrimeType() {
		return crimeType;
	}

	public void setCrimeType(String crimeType) {
		this.crimeType = crimeType;
	}

	@Override
	public int compareTo(CompositeKeyWritable o) {
		// TODO Auto-generated method stub
		int result = location.compareTo(o.location);
        if(result == 0)
        {
            result = crimeType.compareTo(o.crimeType);
        }
        return result;
	}

	@Override
	public void readFields(DataInput di) throws IOException {
		// TODO Auto-generated method stub
		location = WritableUtils.readString(di);
		crimeType = WritableUtils.readString(di);
	}

	@Override
	public void write(DataOutput d) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(d, location);
		WritableUtils.writeString(d, crimeType);
	}
		
	public String toString()
    {
        return (new StringBuilder().append(location).append("\t").append(crimeType).toString());
    }
}
