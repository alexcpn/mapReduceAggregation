package com.nsn.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for KPI Aggregation
 * @author acp
 *
 */
public class KPIReducer extends Reducer<TimeSeriesKey,DoubleWritable,TimeSeriesKey,DoubleWritable>{

	public KPIReducer() {}
	
	
	@Override
	protected void reduce(TimeSeriesKey tskey, Iterable<DoubleWritable> lstdblin,
			Context context)throws IOException, InterruptedException {
		
		double result =0.0;
		Iterator<DoubleWritable> tempItr =  lstdblin.iterator();
		while (tempItr.hasNext()) {
			result = result + ((DoubleWritable) tempItr.next()).get();
		}
		context.write(tskey, new DoubleWritable(result));
	}
	

}
