package com.jum.http;

public class Response {
	
	private int status;
	private String responseStr;
	
	public Response(int responseStatus, String responseStr){
		this.status = responseStatus;
		this.responseStr = responseStr;
	}

	public int getStatus() {
		return status;
	}

	public String getResponseStr() {
		return responseStr;
	}

}
