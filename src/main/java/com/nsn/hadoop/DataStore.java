package com.nsn.hadoop;

import java.util.Date;

public class DataStore {

	public DataStore(){};
	
	int rowid;
	public int getRowid() {
		return rowid;
	}

	public void setRowid(int rowid) {
		this.rowid = rowid;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getKpiName() {
		return kpiName;
	}

	public void setKpiName(String kpiName) {
		this.kpiName = kpiName;
	}

	public Double getKpiValue() {
		return kpiValue;
	}

	public void setKpiValue(Double kpiValue) {
		this.kpiValue = kpiValue;
	}

	Date date;
	String kpiName;
	Double kpiValue;

	public DataStore(int id, Date date,String kpi,double value) {
		this.rowid=id;
		this.date=date;
		this.kpiName=kpi;
		this.kpiValue=value;
	}

}
