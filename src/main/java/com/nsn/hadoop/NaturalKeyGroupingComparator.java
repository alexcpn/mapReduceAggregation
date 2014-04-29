package com.nsn.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {

	public NaturalKeyGroupingComparator() {
		super(TimeSeriesKeyDaily.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		if(a instanceof TimeSeriesKeyFifteenMts){//refactor

			TimeSeriesKeyFifteenMts tskey1 = (TimeSeriesKeyFifteenMts)a;
			TimeSeriesKeyFifteenMts tskey2 = (TimeSeriesKeyFifteenMts)b;
			System.out.println("NaturalKeyGroupingComparator TimeseriesKeyFifteenMts=" + tskey1.getKpi() + " " + tskey2.getKpi());

			int i=	tskey1.getKpi().compareTo(tskey2.getKpi()) ;
			if(i !=0) return i;

			i= tskey1.getTimestamp().compareTo(tskey2.getTimestamp());
			System.out.println("NaturalKeyGroupingComparator Result = "+ i +" Vals= "+ 
					tskey1.getTimestamp() + " " + tskey2.getTimestamp());
			return i;
		}

		TimeSeriesKey tskey1 = (TimeSeriesKey)a;
		TimeSeriesKey tskey2 = (TimeSeriesKey)b;
		System.out.println("NaturalKeyGroupingComparator=" + tskey1.getKpi() + " " + tskey2.getKpi());

		int i=	tskey1.getKpi().compareTo(tskey2.getKpi()) ;
		if(i !=0) return i;

		i= tskey1.getTimestamp().compareTo(tskey2.getTimestamp());
		System.out.println("NaturalKeyGroupingComparator Result = "+ i +" Vals= "+ 
				tskey1.getTimestamp() + " " + tskey2.getTimestamp());
		return i;
	}

}
