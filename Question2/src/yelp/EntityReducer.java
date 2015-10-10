package yelp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EntityReducer extends Reducer<Text, Text,Text, Text>{
	
	public void Reduce(Text key,Text value,Context context) {
		
		
	}

}
