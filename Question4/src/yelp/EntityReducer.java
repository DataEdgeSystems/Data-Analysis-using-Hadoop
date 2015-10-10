package yelp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EntityReducer extends Reducer<Text, IntWritable, Text, Text>{
	
	private DoubleWritable avg_ratings = new DoubleWritable();
//	private HashMap<String,Double> map = new HashMap<String,Double>();
//	private ValueComparator bvc =  new ValueComparator(map);
//	private TreeMap<String,Double> sorted_map = new TreeMap<String,Double>(bvc); 
	private Text word = new Text();
//	private Text val = new Text();
	
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		
		int sum=0;
		int count =0;
		for(IntWritable val :  values){
			
			sum=+val.get();
			count=+1;
		}
		double  avg_rating= sum/count;
		avg_ratings.set(avg_rating);
//		map.put(key.toString(),avg_rating);
		context.write(key,new Text(avg_ratings.toString()));
		
	}
/*	public void cleanup(Context context) throws IOException, InterruptedException {
		
		sorted_map.putAll(map);
		int count=0;
		for (Entry<String, Double> entry : sorted_map.entrySet()) {
	        word.set(entry.getKey());
	        avg_ratings.set(entry.getValue());
	        if(count<10) {
	        	val.set(avg_ratings.toString());
	        	context.write(word,val);
	        	count++;
	        } else {
	        	break;
	        }
		}
		
	}
*/
}
