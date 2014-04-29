package com.nsn.hadoop;

import static junit.framework.Assert.assertEquals;

import java.text.ParseException;
import java.util.Calendar;

import org.junit.Test;

public class TestKPIMapper {
	
	KPIMapper mapper = new KPIMapper();
	
	 
	@Test
	public void testMapper() throws ParseException{
		
		DataStore ds = mapper.parseData("1,10-03-2014 11:00:00,X,1");
		Calendar cal=  Calendar.getInstance();
		cal.setTime(ds.getDate());
		assertEquals(cal.get(Calendar.YEAR),2014);
		assertEquals(cal.get(Calendar.DAY_OF_MONTH),10);
		assertEquals(cal.get(Calendar.MONTH),Calendar.MARCH);
		
		
	}
	
	@Test
	public void testMappar2() throws ParseException{
		
		DataStore ds = mapper.parseData("1,10-03-2014 11:00:00,X,1.5");
		Calendar cal=  Calendar.getInstance();
		cal.setTime(ds.getDate());
		System.out.println(cal.getTime().toString());
		assertEquals(cal.get(Calendar.YEAR),2014);
		assertEquals(cal.get(Calendar.DAY_OF_MONTH),10);
		assertEquals(cal.get(Calendar.MONTH),Calendar.MARCH);
		assertEquals(1.5, ds.getKpiValue());
		assertEquals("X", ds.getKpiName());
	}
	
}

