package com.nsn.hadoop;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper part - For learning
 * @author acp
 *
 */
public class KPIMapper extends Mapper<LongWritable, Text, TimeSeriesKey, DoubleWritable>{

	private enum Aggregation {DAILY, HOURLY,FifteenMinute};
	final Aggregation AGGREGATION= Aggregation.DAILY;
	
	
	public KPIMapper()  {
	
	}


	@Override
	protected void map(LongWritable key, Text value,
			Context context)
					throws IOException, InterruptedException {

		DataStore ds;
		try {
			ds = parseData(value.toString());
		} catch (ParseException e) {
			System.out.println("Error in Parsing Input");
			e.printStackTrace();
			return;
		}

		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(0);
		cal.setTime(ds.getDate());//the original date

		/**
		 * Here is the logic of Aggregating Per DAY
		 * If it is per hour add the hour also here
		 *//*

		int hour=cal.get(Calendar.HOUR_OF_DAY);
		int day=cal.get(Calendar.DAY_OF_MONTH);
		int month=cal.get(Calendar.MONTH);
		int year=cal.get(Calendar.YEAR);

		Calendar caltemp= Calendar.getInstance();
		caltemp.setTimeInMillis(0);

		*//**
		 * WE take only per day and cut off form the rest
		 *//*
		if(AGGREGATION==Aggregation.HOURLY){
			caltemp.set(year, month, day,hour,0,0);
		}else if (AGGREGATION==Aggregation.DAILY){
			caltemp.set(year, month, day,0,0,0);
		}
		
		System.out.println("Input date=" +caltemp.getTime().toString());*/
		TimeSeriesKey ts = new TimeSeriesKeyDaily();
		ts.set(ds.getKpiName(), cal.getTime()); //set the composite key
		System.out.println("Input date=" +cal.getTime().toString());
		context.write(ts,new DoubleWritable(ds.getKpiValue()));

	}

	public DataStore parseData(String data) throws ParseException{

		String[] csvout = data.split(",");

		if(csvout.length!=4){
			System.out.println("Incorrect number of arguments, expected is 4");
			return new DataStore();
		}
		String kpi= csvout[2];

		Double val = Double.parseDouble(csvout[3]);
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse(csvout[1]);

		return  new DataStore(0, date, kpi, val);


	}
}