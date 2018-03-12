package CompanyCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.*;

public class CompanyCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	  private final static IntWritable one = new IntWritable(1);	 
	  private Text record = new Text();
	  
	  public void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException { 
		  int j=0;
		  String line = value.toString();
		  record = value;
		  String[] lineArray = record.toString().split("\\|");
		  
		  if (lineArray[0].equals(null) || lineArray[0].equals("NA")) {
			  j=1;
		  }
		  
		  if (j!=1) {
			  Text t1 = new Text(lineArray[0]);
			  context.write(t1, one);
		  }
	  }
}