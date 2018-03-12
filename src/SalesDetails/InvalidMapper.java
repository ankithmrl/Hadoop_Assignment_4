package SalesDetails;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.*;

public class InvalidMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	  private Text record = new Text();
	  
	  public void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException { 
		  int j = 0;
		  String line = value.toString();
		  record = value;
		  String[] lineArray = record.toString().split("\\|");
		  
		  for (int i=0; i< lineArray.length; i++) {
			  if (lineArray[i].equals("NA")) {
				  j=1;
			  }
		  }
		  if (j!=1) {
			  context.write(record, null);
		  }
	  }
}