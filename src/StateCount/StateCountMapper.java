package StateCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.*;

public class StateCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	  private final static IntWritable one = new IntWritable(1);	 
	  private Text record = new Text();
	  
	  public void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException { 
		  int j=0;
		  String line = value.toString();
		  record = value;
		  String[] lineArray = record.toString().split("\\|");
		  
		  if (lineArray[0].equals("Onida")) {
			  // Gets the State name only for Onida company.
			  Text t1 = new Text(lineArray[3]);
			  context.write(t1, one);
		  }
	  }
}