package com.jum.utils;

import au.com.bytecode.opencsv.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class CsvDataProvider implements Iterator<Object[]>{

	private static final Logger logger = LoggerFactory.getLogger(CsvDataProvider.class);

	private List tableList;

	private CSVReader csvr;

	private int rowNum = 0;

	private int curRowNo = 0;

	private int columnNum = 0;

	private String[] columnName;

	public CsvDataProvider(String fileName) {
		try {
			File csv = new File(fileName);
			InputStreamReader isr = new InputStreamReader(new FileInputStream(csv), "utf-8");
			csvr = new CSVReader(isr);
			tableList = csvr.readAll();
			columnName = (String[]) tableList.get(0);
			this.rowNum = tableList.size();
			this.columnNum = columnName.length;
			this.curRowNo++;
		} catch (IOException e) {
			logger.error("File don't exist, or exceptions occurs when open this file.", e);
		}

	}

	public void setRunCase(){
		List<String []> templList= new ArrayList<String []>();
		templList.addAll(tableList);
		for (int i = templList.size()-1; i >0; i--) {
			if (String.valueOf(templList.get(i)[0]).equals("N")) {
				tableList.remove(i);//将标记为"N"的用例移除tablelist，同时将行号减一
				rowNum--;
			}
		}
		
	}
	// @Override
	public boolean hasNext() {
		if (this.rowNum == 0 || this.curRowNo >= this.rowNum) {
			try {
				csvr.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return false;
		} else {
			return true;
		}
	}

	// @Override
	public Object[] next() {
		/*
		 * 将数据放入map
		 */
		Map<String, String> s = new LinkedHashMap<String, String>();
		String Nextline[] = (String[]) tableList.get(curRowNo);
		List<String> keys = Arrays.asList(Nextline);
		if (keys.size() > this.columnNum) {
			logger.error("当前行的列数大于csv文件中第一列的个数，请仔细核对");
	//		System.exit(0);
			return null;
		}

		for (int i = 0; i < this.columnNum; i++) {
			String temp = "";
			try {
				temp = Nextline[i].toString();
			} catch (ArrayIndexOutOfBoundsException ex) {
				temp = "";
			}
			s.put(this.columnName[i], temp);
		}
		Object r[] = new Object[1];
		r[0] = s;
		this.curRowNo++;
		return (Object[]) r;
	}

	// @Override
	public void remove() {
		throw new UnsupportedOperationException("remove unsupported");
	}

}
