package com.nsn.hadoop;

import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

public interface TimeSeriesKey extends WritableComparable<TimeSeriesKey>  {

	

	public String getKpi();
	public Date getTimestamp();

	public void set(String kpiName, Date time);
}
