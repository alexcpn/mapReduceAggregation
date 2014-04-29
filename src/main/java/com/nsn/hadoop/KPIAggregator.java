package com.nsn.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class KPIAggregator {

	public KPIAggregator() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		  Configuration conf = new Configuration();
		  Job job = new Job(conf, "KPI_Aggregator");
		  job.setJarByClass(KPIAggregator.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(TimeSeriesKeyAbs.class);
		  
		  job.setMapperClass(KPIMapper.class);
		  
		  String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		  

	}

}
