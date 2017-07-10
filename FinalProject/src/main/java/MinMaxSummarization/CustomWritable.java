package MinMaxSummarization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class CustomWritable implements Writable{
  
    private String type;
    private int count;
    
    public CustomWritable()
    {
        
    }

    public CustomWritable(String t, int c)
    {
        this.type = t;
        this.count = c;
    }
    
     @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeString(d, type);
        WritableUtils.writeVInt(d, count);

    }

    @Override
    public void readFields(DataInput di) throws IOException {
        type = WritableUtils.readString(di);
        count = WritableUtils.readVInt(di);
    }

		public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

		public String toString()
    {
        return ("");
    }
}
