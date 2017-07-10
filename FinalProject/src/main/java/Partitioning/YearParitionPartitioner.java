package Partitioning;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearParitionPartitioner extends Partitioner<Text, Text> implements Configurable {

	private Configuration conf = null;
	 
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		 this.conf = arg0;
	}

	@Override
	public int getPartition(Text key, Text value, int i) {
		// TODO Auto-generated method stub
		String str = key.toString();
	       switch(str)
	       {
	           case "2011":
	               return 0;
	           case "2012":
	               return 1;
	           case "2013":
	               return 2;
	           case "2014":
	               return 3;
	           case "2015":
	               return 4;
	           case "2016":
	               return 5;
	           case "2017":
	               return 6;
	       }
	       return 7;
	}

	
}
