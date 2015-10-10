package yelp;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class BusinessReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	
	private DoubleWritable avg_ratings = new DoubleWritable();
	private HashMap<String,Double> map = new HashMap<String,Double>();
	private ValueComparator bvc =  new ValueComparator(map);
	private TreeMap<String,Double> sorted_map = new TreeMap<String,Double>(bvc); 
	private Text word = new Text();

		
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
		String joinkey = key.toString();
		String full_address = new String();
		String catagory = new String();
		int count = 0;
		double avg_rating = new Double(0.0);
		
		for(Text val : values) { 
			
			String recordString = val.toString();
			String [] record = recordString.split("::");
			if(record.length==2 && record[0].equalsIgnoreCase("review")) {
				avg_rating=Double.parseDouble(record[1]);
			} else if (record.length==2 && record[0].equalsIgnoreCase("business")) {
				full_address=record[1];
			} else if (record.length==3){
				full_address=record[1];
				catagory=record[2];
			}
			count++;
		}
		if(count>1 && catagory!=null){
			//context.write(key,new Text(full_address+" "+catagory+" "+avg_ratings));
			map.put(key.toString()+" "+full_address+" "+catagory, avg_rating);
		}
	}
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		sorted_map.putAll(map);
		int count=0;
		for (Entry<String, Double> entry : sorted_map.entrySet()) {
	        word.set(entry.getKey());
	        avg_ratings.set(entry.getValue());
	        if(count<10) {
	        	context.write(word,avg_ratings);
	        	count++;
	        } else {
	        	break;
	        }
		}
		
	}
	
	
}
