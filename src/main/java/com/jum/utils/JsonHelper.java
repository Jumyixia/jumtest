package com.jum.utils;

import com.alibaba.fastjson.JSON;

public class JsonHelper {
	public static String objectToString(Object obj)
	  {
	    return JSON.toJSONString(obj);
	  }
	  
	  public static <T> T stringToObject(String jsonString, Class<T> pojoClass)
	  {
	    return (T) JSON.parseObject(jsonString, pojoClass);
	  }
}
