package com.nsn.hadoop;

import java.util.Calendar;

/**
 * This is a composite key
 * @author acp
 *
 */
public class TimeSeriesKeyFifteenMts extends TimeSeriesKeyAbs {
	
	@Override
	public int compareTo(TimeSeriesKey other) {

		if( this.kpi.compareTo(other.getKpi())!=0){ // compare keys
			return this.kpi.compareTo(other.getKpi());
		}

		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(0);
		cal.setTime(this.time);

		Calendar cal2 = Calendar.getInstance();
		cal2.setTimeInMillis(0);
		cal2.setTime(other.getTimestamp());

		System.out.println("In Compareto--" + cal.getTime().toString() + " --" + cal2.getTime().toString() );
		int i=cal.get(Calendar.YEAR)- cal2.get(Calendar.YEAR);
		if(i!=0) return ( i > 0 ? 1 :-1);
		System.out.println("In Compareto Year is same");
		i=cal.get(Calendar.MONTH)-cal2.get(Calendar.MONTH) ;
		if(i!=0) return ( i > 0 ? 1 :-1);
		System.out.println("In Compareto Month is same");
		i=cal.get(Calendar.DAY_OF_MONTH)- cal2.get(Calendar.DAY_OF_MONTH);
		if(i!=0) return ( i > 0 ? 1 :-1);
		System.out.println("In Compareto Day is same");
		i=cal.get(Calendar.HOUR_OF_DAY) - cal2.get(Calendar.HOUR_OF_DAY);
		if(i!=0) return ( i > 0 ? 1 :-1);
		System.out.println("In Compareto Hour is same");
		/**
		 * Check here if it is in 15 mts interval
		 */
		i=(cal.get(Calendar.MINUTE) / 15) -(cal.get(Calendar.MINUTE)/ 15) ;
		System.out.println("In Compareto Minute is " + i);
		return ( i > 0 ? 1 : (i < 0 ? -1:1));
	}
	

}
