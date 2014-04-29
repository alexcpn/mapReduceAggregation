package com.nsn.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class TestKPIAggregatorFifteenMinute 
{
	MapReduceDriver<LongWritable, Text, TimeSeriesKey, DoubleWritable, TimeSeriesKey, DoubleWritable> mapReduceDriver;
	MapDriver<LongWritable, Text, TimeSeriesKey, DoubleWritable> mapDriver;
	ReduceDriver<TimeSeriesKey,DoubleWritable,TimeSeriesKey,DoubleWritable> reduceDriver;


	public TestKPIAggregatorFifteenMinute() {

	}

	@Before
	public void setup(){
		KPIMapper mapper =  new KPIMapper();
		KPIReducer reducer = new KPIReducer();
		NaturalKeyGroupingComparator groupingComparator = new NaturalKeyGroupingComparator();
		mapDriver = new MapDriver<LongWritable, Text, TimeSeriesKey, DoubleWritable>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<TimeSeriesKey,DoubleWritable,TimeSeriesKey,DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		mapReduceDriver.setKeyGroupingComparator(groupingComparator);

	}

	@Test
	public void testMapperDaily() throws IOException, ParseException{

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);

		mapDriver.withInput(new LongWritable(1), new Text("1,10-03-2014 11:00:00,X,1.0"))
		.withInput(new LongWritable(2), new Text("2,10-03-2014 11:00:00,X,1.5"))
		.withInput(new LongWritable(3), new Text("3,10-03-2014 11:00:00,X,2"))
		.withOutput(tskey, new DoubleWritable(1.0))
		.withOutput(tskey, new DoubleWritable(1.5))
		.withOutput(tskey, new DoubleWritable(2.0));
		mapDriver.runTest();
		/* List<Pair<TimeSeriesKey,DoubleWritable>> temp= mapDriver.getExpectedOutputs();
	     for(Pair<TimeSeriesKey,DoubleWritable> itr : temp){
	    	 assertEquals(itr.getFirst(),tskey);
	     }*/
	}

	@Test
	public void testReducerDaily() throws ParseException, IOException{

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(1.5));
		values.add(new DoubleWritable(2.2));

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);

		reduceDriver.withInput(tskey,values);
		reduceDriver.withOutput(tskey, new DoubleWritable(3.7));
		reduceDriver.runTest();
	}
	/*
	 * Testing differnt date , but same KPI (aggregating per day)
	 * 
	 */
	@Test
	public void testReducer2Daily() throws ParseException, IOException{

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(1.5));
		values.add(new DoubleWritable(2.2));

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);

		TimeSeriesKey tskey2 = new TimeSeriesKeyFifteenMts();
		Date date2 = df.parse("11-03-2014  11:00:00"); 
		tskey2.set("X", date2);

		List<DoubleWritable> values2 = new ArrayList<DoubleWritable>();
		values2.add(new DoubleWritable(1.1));
		values2.add(new DoubleWritable(2.1));


		reduceDriver.withInput(tskey,values)
		.withInput(tskey2,values2)
		.withOutput(tskey, new DoubleWritable(3.7))
		.withOutput(tskey2, new DoubleWritable(3.2));

		reduceDriver.runTest();

	}
	
	/*
	 * Testing different date time , but same KPI (aggregating per day)
	 * 
	 */
	@Test
	public void testReducer5Daily() throws ParseException, IOException{

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(1.5));
		values.add(new DoubleWritable(2.2));

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);

		TimeSeriesKey tskey2 = new TimeSeriesKeyFifteenMts();
		Date date2 = df.parse("10-03-2014 11:00:00"); 
		tskey2.set("X", date2);

		List<DoubleWritable> values2 = new ArrayList<DoubleWritable>();
		values2.add(new DoubleWritable(1.1));
		values2.add(new DoubleWritable(2.1));


		reduceDriver.withInput(tskey,values)
		.withInput(tskey2,values2)
		.withOutput(tskey, new DoubleWritable(3.7))
		.withOutput(tskey2, new DoubleWritable(3.2));

		reduceDriver.runTest();

	}

	/**
	 * Testing same date different KPI (grouping per KPI)
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void testReducer3Daily() throws ParseException, IOException{

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(1.5));
		values.add(new DoubleWritable(2.2));

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);

		TimeSeriesKey tskey2 = new TimeSeriesKeyFifteenMts();

		Date date2 = df.parse("10-03-2014 11:00:00");
		tskey2.set("Y", date2);

		List<DoubleWritable> values2 = new ArrayList<DoubleWritable>();
		values2.add(new DoubleWritable(1.1));
		values2.add(new DoubleWritable(2.1));


		reduceDriver.withInput(tskey,values)
		.withInput(tskey2,values2)
		.withOutput(tskey, new DoubleWritable(3.7))
		.withOutput(tskey2, new DoubleWritable(3.2));

		reduceDriver.runTest();

	}

	@Test(expected=AssertionError.class)
	public void testReducer4Daily() throws ParseException, IOException{

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(1.5));
		values.add(new DoubleWritable(2.2));

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);

		TimeSeriesKey tskey2 = new TimeSeriesKeyFifteenMts();

		Date date2 = df.parse("10-03-2014 11:00:00");
		tskey2.set("X", date2);

		List<DoubleWritable> values2 = new ArrayList<DoubleWritable>();
		values2.add(new DoubleWritable(1.1));
		values2.add(new DoubleWritable(2.1));

		assertEquals(tskey,tskey2);
		reduceDriver
		.withInput(tskey,values)
		.withInput(tskey2,values2)
		.withOutput(tskey, new DoubleWritable(6.9));//will fail

		reduceDriver.runTest();

	}

	@Test
	public void testMapReduceDaily() throws ParseException, IOException {

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);

		mapReduceDriver
		.withInput(new LongWritable(1), new Text("1,10-03-2014 11:00:00,X,1.5"))
		.withInput(new LongWritable(2), new Text("2,10-03-2014 11:00:00,X,2.2"))
		.withInput(new LongWritable(3), new Text("3,10-03-2014 11:00:00,X,1.1"))
		.withInput(new LongWritable(3), new Text("3,10-03-2014 11:00:00,X,2.1"))
		.withOutput(tskey, new DoubleWritable(6.9));

		mapReduceDriver.runTest();

	}
	
	@Test
	public void testMapReduceDaily4() throws ParseException, IOException {

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);
		Date date2 = df.parse("10-03-2014 11:15:00");
		TimeSeriesKey tskey2 = new TimeSeriesKeyFifteenMts();
		tskey2.set("X", date2);

		mapReduceDriver
		.withInput(new LongWritable(1), new Text("1,10-03-2014 11:00:00,X,1.5"))
		.withInput(new LongWritable(2), new Text("2,10-03-2014 11:00:00,X,2.2"))
		.withInput(new LongWritable(3), new Text("3,10-03-2014 11:15:00,X,1.1"))
		.withInput(new LongWritable(3), new Text("3,10-03-2014 11:15:00,X,2.1"))
		.withOutput(tskey, new DoubleWritable(3.7))
		.withOutput(tskey2, new DoubleWritable(3.2));

		mapReduceDriver.runTest();

	}
	
	@Test
	public void testMapReduceDaily5() throws ParseException, IOException {

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);
		Date date2 = df.parse("10-03-2014 11:15:00");
		TimeSeriesKey tskey2 = new TimeSeriesKeyFifteenMts();
		tskey2.set("X", date2);
		Date date3 = df.parse("10-03-2014 11:15:00");
		TimeSeriesKey tskey3 = new TimeSeriesKeyFifteenMts();
		tskey3.set("Y", date3);
		Date date4 = df.parse("10-03-2014 11:25:00");
		TimeSeriesKey tskey4 = new TimeSeriesKeyFifteenMts();
		tskey4.set("Y", date4);

		mapReduceDriver
		.withInput(new LongWritable(1), new Text("1,10-03-2014 11:00:00,X,1.5"))
		.withInput(new LongWritable(2), new Text("2,10-03-2014 11:00:00,X,2.2"))
		.withInput(new LongWritable(3), new Text("3,10-03-2014 11:15:00,X,1.1"))
		.withInput(new LongWritable(4), new Text("3,10-03-2014 11:15:00,X,2.1"))
		.withInput(new LongWritable(5), new Text("3,10-03-2014 11:15:00,Y,2.1"))
		.withInput(new LongWritable(6), new Text("3,10-03-2014 11:25:00,Y,2.3"))
		.withOutput(tskey, new DoubleWritable(3.7))
		.withOutput(tskey2, new DoubleWritable(3.2))
		.withOutput(tskey3, new DoubleWritable(2.1))
		.withOutput(tskey4, new DoubleWritable(2.3))
		;

		mapReduceDriver.runTest();

	}

	@Test
	public void testMapReduceDaily2() throws ParseException, IOException {

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);

		TimeSeriesKey tskey2 = new TimeSeriesKeyFifteenMts();
		Date date2 = df.parse("11-03-2014 11:00:00");
		tskey2.set("X", date2);

		mapReduceDriver
		.withInput(new LongWritable(1), new Text("1,10-03-2014 11:00:00,X,1.5"))
		.withInput(new LongWritable(2), new Text("2,10-03-2014 11:00:00,X,2.2"))
		.withInput(new LongWritable(3), new Text("3,11-03-2014 11:00:00,X,1.1"))
		.withInput(new LongWritable(4), new Text("3,11-03-2014 11:00:00,X,2.1"))
		.withOutput(tskey, new DoubleWritable(3.7))
		.withOutput(tskey2, new DoubleWritable(3.2));

		mapReduceDriver.runTest();

	}

	/**
	 * Per Day Aggregation
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void testMapReduceDaily3() throws ParseException, IOException {

		TimeSeriesKey tskey = new TimeSeriesKeyFifteenMts();
		DateFormat df= new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		Date date = df.parse("10-03-2014 11:00:00");
		tskey.set("X", date);

		TimeSeriesKey tskey2 = new TimeSeriesKeyFifteenMts();
		Date date2 = df.parse("11-03-2014 11:00:00");
		tskey2.set("X", date2);

		TimeSeriesKey tskey3 = new TimeSeriesKeyFifteenMts();
		tskey3.set("Y", date2);

		mapReduceDriver
		.withInput(new LongWritable(1), new Text("1,10-03-2014 11:00:00,X,1.5"))
		.withInput(new LongWritable(2), new Text("2,10-03-2014 11:00:00,X,2.2"))
		.withInput(new LongWritable(3), new Text("3,11-03-2014 11:00:00,X,1.1"))
		.withInput(new LongWritable(3), new Text("3,11-03-2014 11:00:00,Y,2.1"))
		.withOutput(tskey, new DoubleWritable(3.7))
		.withOutput(tskey2, new DoubleWritable(1.1))
		.withOutput(tskey3, new DoubleWritable(2.1));

		mapReduceDriver.runTest();

	}
}
