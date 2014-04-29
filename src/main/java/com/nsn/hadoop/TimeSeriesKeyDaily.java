package com.nsn.hadoop;

import java.util.Calendar;

public class TimeSeriesKeyDaily extends TimeSeriesKeyAbs {
 
	@Override
	public int compareTo(TimeSeriesKey other) {
		
		/*System.out.println("In Compareto.......");
		if( this.kpi.compareTo(other.getKpi())!=0){ // compare keys
			return this.kpi.compareTo(other.getKpi());
		}else if  (this.time.compareTo(other.getTimestamp())!=0){ //If KPI's are the same then order by time
			return (this.time.compareTo(other.getTimestamp()));
		}else{
			return 0;
		}*/
		
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(0);
		cal.setTime(this.time);

		Calendar cal2 = Calendar.getInstance();
		cal2.setTimeInMillis(0);
		cal2.setTime(other.getTimestamp());

		System.out.println("TimeSeriesKeyDaily In Compareto--" + cal.getTime().toString() + " --" + cal2.getTime().toString() );
		int i=cal.get(Calendar.YEAR)- cal2.get(Calendar.YEAR);
		if(i!=0) return ( i > 0 ? 1 :-1);
		System.out.println("In Compareto Year is same");
		i=cal.get(Calendar.MONTH)-cal2.get(Calendar.MONTH) ;
		if(i!=0) return ( i > 0 ? 1 :-1);
		System.out.println("In Compareto Month is same");
		i=cal.get(Calendar.DAY_OF_MONTH)- cal2.get(Calendar.DAY_OF_MONTH);
		return ( i > 0 ? 1 : (i < 0 ? -1:0));
	}

}
