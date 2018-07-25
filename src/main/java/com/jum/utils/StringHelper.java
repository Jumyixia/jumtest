package com.jum.utils;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringHelper extends StringUtils {
	private static final Logger logger = LoggerFactory.getLogger(StringHelper.class);
	public static final String EMPTY_STRING = "";
	public static final String NULL_STRING = "NULL";

	public static Date convert2Date(String input) {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		if (null == input) {
			return date;
		}
		try {
			date = format.parse(input);
		} catch (ParseException e) {
			logger.error("", e);
		}
		return date;
	}

	public static boolean convert2Boolean(String input) {
		if (null == input) {
			return false;
		}
		return Boolean.parseBoolean(input);
	}

	public static String convert2String(String src) {
		if (null == src) {
			return null;
		}
		if (src.equalsIgnoreCase("NULL")) {
			return null;
		}
		return src;
	}

	public static Map<String, String> convert2Map(String context) {
		if (null == context) {
			return null;
		}
		if (context.equalsIgnoreCase("NULL")) {
			return null;
		}
		Map<String, String> map = new HashMap();
		String[] kv = context.split(";");
		for (int i = 0; i < kv.length; i++) {
			String[] key = kv[i].split("=");
			if (key.length > 1) {
				map.put(key[0], key[1]);
			}
		}
		return map;
	}

	public static List<String> convert2List(String ruleUuids) {
		if ((null == ruleUuids) || (ruleUuids.equalsIgnoreCase("NULL"))) {
			return null;
		}
		return Arrays.asList(ruleUuids.split(";"));
	}

	public static Set<String> convert2Set(String str) {
		if ("NULL".equalsIgnoreCase(str)) {
			return null;
		}
		String[] kv = str.split(";");
		return new HashSet(Arrays.asList(kv));
	}

	public static Map<Integer, String> convert2MapInteger(String context) {
		if (null == context) {
			return new HashMap();
		}
		if (context.equalsIgnoreCase("NULL")) {
			return null;
		}
		Map<Integer, String> map = new HashMap();
		String[] kv = context.split(";");
		for (int i = 0; i < kv.length; i++) {
			String[] key = kv[i].split("=");
			if (key.length > 1) {
				map.put(Integer.valueOf(key[0]), key[1]);
			}
		}
		return map;
	}

	public static int convert2int(String input) {
		if ((null == input) || (input.trim() == "")) {
			return 1;
		}
		try {
			return Integer.parseInt(input);
		} catch (Exception e) {
		}
		return 1;
	}
	
	public static Long convert2Long(String input) {
		if ((null == input) || (input.trim() == "")||input.trim() == "null") {
			return null;
		}
		try {
			return Long.parseLong(input);
		} catch (Exception e) {
		}
		return null;
	}

	public static boolean isDigital(String num) {
		return true;
	}

	public static boolean isInteger(String num) {
		if (StringUtils.isBlank(num)) {
			return false;
		}
		if (StringUtils.isNumeric(num)) {
			return true;
		}
		if (("-".equals(num.substring(0, 1))) && (StringUtils.isNumeric(num.substring(1)))) {
			return true;
		}
		return false;
	}

	public static boolean isNumberPosiNega(String num) {
		if (StringUtils.isNumeric(num)) {
			return true;
		}
		if ((StringUtils.isBlank(num)) || (num.length() < 2)) {
			return false;
		}
		if (("-".equals(num.substring(0, 1))) && (StringUtils.isNumeric(num.substring(1)))) {
			return true;
		}
		return false;
	}

	public static boolean isDateFunction(String str) {
		if (StringUtils.isBlank(str)) {
			return false;
		}
		if ((str.contains("(")) && (str.contains(")"))) {
			return true;
		}
		return false;
	}

	public static boolean isBlankOrNull(String str) {
		if ((null == str) || ("".equals(str))) {
			return true;
		}
		return false;
	}

	public static String replaceQuestMark(String str, String... values) {
		String retStr = str;
		if ((null != str) && (!"".equals(str)) && (null != values)) {
			if (getSubstrCountInStr(str) != values.length) {
				logger.error("");
			}
			for (String value : values) {
				retStr = retStr.replaceFirst("\\?", value);
			}
		}
		return retStr;
	}

	public static int getSubstrCountInStr(String str) {
		int count = 0;
		if (null != str) {
			if (str.lastIndexOf("?") == str.length() - 1) {
				count = str.split("\\?").length;
			} else {
				count = str.split("\\?").length - 1;
			}
		}
		return count;
	}

	public static String formatInt(int num, String format) {
		return String.valueOf(new DecimalFormat(format).format(num));
	}

	public static Date getDateByNumericString(String num) {
		if ("NULL".equalsIgnoreCase(num)) {
			return null;
		}
		int per = 0;
		if (isNumberPosiNega(num)) {
			per = Integer.parseInt(num);
		} else {
			per = 7;
		}
		return DateUtils.addDays(new Date(), per);
	}

	public static boolean isDateString(String string) {
		String reg = "\\d{4}(/|-)\\d{1,2}(/|-)\\d{1,2}( +(\\d{1,2}:\\d{1,2}:\\d{1,2})?([.: ]\\d+)?)?";
		Pattern p = Pattern.compile(reg);
		Matcher m = p.matcher(string);
		return m.matches();
	}

	public static String buildErrorLog(String caseId, String description, Object... parameters) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("Catch an exception when executing testcase[");
		buffer.append(caseId);
		buffer.append("]--[");
		buffer.append(description);
		buffer.append("] Calling parameters [");
		for (Object param : parameters) {
			buffer.append(JSON.toJSONString(param));
			buffer.append(",");
		}
		if (parameters.length > 0) {
			buffer.deleteCharAt(buffer.length() - 1);
		}
		buffer.append("]");

		return buffer.toString();
	}

	public static String buildAssertExceptionTips(String caseId, String description) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("Testcase[");
		buffer.append(caseId);
		buffer.append("]--[");
		buffer.append(description);
		buffer.append("] failed because of encounter an unexpected exception.");
		return buffer.toString();
	}

	public static String getAssertPrepeareTips() {
		return "Test data prepare failed, all testcases are skipped.";
	}

	public static String getAssertServiceTips() {
		return "Service to be tested is null, all testcases are skipped.";
	}

	public static String buildAssertCheckTips(String caseId, String description, Object... parameters) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("Checking test result of [");
		buffer.append(caseId);
		buffer.append("]--[");
		buffer.append(description);
		buffer.append("] Calling parameters [");
		for (Object param : parameters) {
			buffer.append(JSON.toJSONString(param));
			buffer.append(",");
		}
		if (parameters.length > 0) {
			buffer.deleteCharAt(buffer.length() - 1);
		}
		buffer.append("] failed, check the error log to know detail.");

		return buffer.toString();
	}

	public static String getNormalTestCSVFilePath(String fileName, Class clazz) {
		String path = "./src/test/resources/testres/normal/";
		String name = clazz.getSimpleName();
		int index = name.lastIndexOf("NormalTest");
		if (index > 0) {
			name = name.substring(0, index);
		}
		if (name.length() > 0) {
			path = path + name;
		}
		return path + "/" + fileName;
	}

	public static boolean isDevHost(String hostname) {
		String reg = ".*\\.[td]\\d{4}aqcn\\.alipay\\.net";
		Pattern p = Pattern.compile(reg);
		Matcher m = p.matcher(hostname);
		return m.matches();
	}
	
	public static String convert2String(List<String> src) {
		StringBuilder sb = new StringBuilder();
		if (null == src) {
			return null;
		}
		if (src.size()==0) {
			return "";
		}
		for (String str : src) {
			sb.append("'").append(str).append("'").append(",");
		}
		return sb.substring(0, sb.lastIndexOf(","));
		
	}

	public static void main(String arg[]) {
		List<String> se  = new ArrayList<String>();
		se.add("B2");
		se.add("nn");
		System.out.println(StringHelper.convert2String(se));
	}
}
