package com.nsn.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

/**
 * This is a composite key
 * @author acp
 *
 */
public abstract class TimeSeriesKeyAbs implements TimeSeriesKey {

	protected String kpi="";
	protected Date time ;

	public void set(String kpi,Date date){
		this.kpi = kpi;
		this.time =date;
	}

	public String getKpi(){

		return kpi;
	}

	public Date getTimestamp(){

		return time;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.kpi =in.readUTF();
		this.time = new Date(in.readLong());
		//System.out.println("readFields=" + this.kpi + ", " + this.time.toString());

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.kpi);
		out.writeLong(this.time.getTime());
		//System.out.println("writeFields=" + this.kpi + ", " + this.time.toString());
	}

	
	@Override
	public boolean equals(Object aThat){
		if(this==aThat){return true;}
		if (!(aThat instanceof TimeSeriesKeyAbs)){return false;};

		TimeSeriesKeyAbs that = (TimeSeriesKeyAbs)aThat;
		return 
				(this.kpi.equals(that.kpi))&&
				(this.time.compareTo(that.time) !=0 ? false :true);//Date equals is tricky

	}

	@Override
	public int hashCode(){

		int result = 0;
		result = result + (kpi!=null ?kpi.hashCode():0);
		result = result + (time!=null ?time.toString().hashCode():0);
		return result;
	}

}
