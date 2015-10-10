package yelp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text val, int num_reducers) {
		
		String joinKey = key.toString().split("::")[0];
		return joinKey.hashCode() % num_reducers;
	}
	

}
