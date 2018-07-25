package com.jum.utils;

public class ColumnInfo {

	private String name;
	private String dbType;
	private int size;
	private String javaType;

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDbType() {
		return this.dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

	public int getSize() {
		return this.size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public String getJavaType() {
		return this.javaType;
	}

	public void setJavaType(String javaType) {
		this.javaType = javaType;
	}

}
