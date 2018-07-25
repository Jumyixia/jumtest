package com.jum.http;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class HttpContent {
	
	private static final Logger logger = Logger.getLogger(HttpContent.class);
	
	private String AppKey;
	private Long CurTime;
	private String CheckSum;
	private Long Nonce;
	
	private String Date;
	private String body;
	
	/**
	 * HttpContent need three parameters : date, contentType, and request body
	 */
	public HttpContent(String date, String body){
		this.Date = date;
		this.body = body;
	}
	
	public HttpContent(String appKey, Long curTime, String checkSum,
                       String date, String body){
		this.AppKey = appKey;
		this.CurTime = curTime;
		this.CheckSum = checkSum;
		
		this.Date = date;
		this.body = body;
	}
	
	public HttpContent(String appKey, Long curTime, String checkSum,
                       Long nonce, String date, String body){
		this.AppKey = appKey;
		this.CurTime = curTime;
		this.CheckSum = checkSum;
		this.Nonce = nonce;
		
		this.Date = date;
		this.body = body;
	}
	// lack of appKey
	public HttpContent(Long curTime, String checkSum, Long nonce,
                       String date, String body){
		this.CurTime = curTime;
		this.CheckSum = checkSum;
		this.Nonce = nonce;
		
		this.Date = date;
		this.body = body;
	}
	
	// lack of curTime
	public HttpContent(String appKey, int noCurTime, String checkSum,
                       Long nonce, String date, String body){
		this.AppKey = appKey;
		this.CheckSum = checkSum;
		this.Nonce = nonce;
		
		this.Date = date;
		this.body = body;
	}
	
	// lack of checkSum
	public HttpContent(String appKey, Long curTime, int noCheckSum,
                       Long nonce, String date, String body){
		this.AppKey = appKey;
		this.CurTime = curTime;
		this.Nonce = nonce;
		
		this.Date = date;
		this.body = body;
	}
	
	public String toJson(){
		
		return SingleInstance.gson.toJson(this);
	}
	
	/**
	 * get http header, f.g, date , contentType
	 * @return
	 */
	public Map<String, String> getHttpHeader(){
		
		String oldJsonString = SingleInstance.gson.toJson(this);		
		JsonObject resp = new JsonParser().parse(oldJsonString).getAsJsonObject();
		
		Map<String, String> header = new HashMap<String, String>();
		for(Map.Entry<String, JsonElement> element:resp.entrySet())
			header.put(element.getKey(), element.getValue().getAsString());

		return header;
	}
	
	/**
	 * get body of HTTP message
	 * @return
	 */
	public String getHttpBody(){
		return this.body;
	}
	
	/**
	 * return true if http header is valid, otherwise false
	 * @return
	 */
	public boolean httpHeaderIsValid(){
		Map<String, String> header = getHttpHeader();
		if(header == null || header.isEmpty() == true)
			return false;
		else
			return true;
	}

}
