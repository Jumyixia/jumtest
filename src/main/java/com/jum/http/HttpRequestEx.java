package com.jum.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpRequestEx {
	
	private static final Logger logger = Logger.getLogger(HttpRequestEx.class);
	private String host;
    private HttpClient httpClient = new DefaultHttpClient();

	public HttpRequestEx(){
		super();
	}

    /**
     * this constructor initialize with host and DefaulthttpClient
     */
	public HttpRequestEx(String host){
		this.host = host;
	}
	
	/**
	 * this constructor initialize with host and specified httpClient
	 * @param host
	 * @param httpClient
	 */
	public HttpRequestEx(String host, HttpClient httpClient){
		this.host = host;
		this.httpClient = httpClient;
	}
	
	
	public void ShutDown(){
		if (httpClient != null)
			httpClient.getConnectionManager().shutdown();
		else
			logger.info("fail to shut down HTTP connection in HttpRequest");
	}
	
	/**
	 * get complete URL with htttp protocol
	 * @param path
	 * @param query
	 * @return
	 */
	private String getCompleteURL(List<String> path, Map<String, String> query){
		StringBuilder url = new StringBuilder("http://" + host);
		if(path == null || path.isEmpty() == true){
			logger.error("the path is invalid");
			return null;
		}
		for(String element:path)
			url.append("/" + element);
		
		if(query == null || query.isEmpty() == true){
			logger.info("the URL is : " + url.toString());
			return url.toString();
		}
		url.append("?");
		for(Map.Entry<String, String> entry:query.entrySet()){
			url.append(entry.getKey() + "=" + entry.getValue() + "&");
		}
		url.deleteCharAt(url.length() - 1);
		logger.info("the URL is : " + url.toString());
		return url.toString();
	}
	
	
	private static Response processResponse(HttpResponse httpResponse) {
		Response response = null;
		try{
			int statusCode = httpResponse.getStatusLine().getStatusCode();			
			HttpEntity entity = httpResponse.getEntity();
			
			ByteArrayOutputStream arrayStream = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			InputStream is = entity.getContent();
			
			int len;
			while ((len = is.read(buffer)) > 0) {
				arrayStream.write(buffer, 0, len);
			}
			
			String responseStr = new String(arrayStream.toByteArray(), "UTF-8");
			is.close();
			EntityUtils.consume(entity);
						
			logger.info(statusCode);
			logger.info(httpResponse.getStatusLine());
			logger.info(responseStr);	
			
			response = new Response(statusCode, responseStr);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return response;
	}	
	
	public Response put(List<String> path, Map<String, String> query, HttpContent httpContent) throws IOException {

		HttpPut httpPut = new HttpPut(getCompleteURL(path, query));	
		
		if( !httpContent.httpHeaderIsValid() ){
			logger.error("http header is invalid");
			return null;
		}
		for(Map.Entry<String, String> entry:httpContent.getHttpHeader().entrySet())
			httpPut.addHeader(entry.getKey(), entry.getValue());
		
		httpPut.addHeader("Content-Type", "application/json;charset=utf-8");
		
		httpPut.setEntity(new StringEntity(httpContent.getHttpBody(), "UTF-8"));
		
		logger.info("http header is : " + httpContent.getHttpHeader());
		logger.info("http body is : " + httpContent.getHttpBody());
		
		Response response = processResponse(httpClient.execute(httpPut));	
		
		return response;
		
	}
	
	public Response post(List<String> path, Map<String, String> query, HttpContent httpContent) throws IOException {

		HttpPost httpPost = new HttpPost(getCompleteURL(path, query));	
		
		if( !httpContent.httpHeaderIsValid() ){
			logger.error("http header is invalid");
			return null;
		}
		if(httpContent.getHttpBody() == null){
			logger.error("http body is null which is invalid");
			return null;
		}
		
		for(Map.Entry<String, String> entry:httpContent.getHttpHeader().entrySet())
			httpPost.addHeader(entry.getKey(), entry.getValue());

		httpPost.addHeader("Content-Type", "application/json;charset=utf-8");
		
		httpPost.setEntity(new StringEntity(httpContent.getHttpBody(), "UTF-8"));
		
		logger.info("http header is : " + httpContent.getHttpHeader());
		logger.info("http body is : " + httpContent.getHttpBody());
		
		Response response = processResponse(httpClient.execute(httpPost));	
		
		return response;
		
	}
	
	/**
	 * HTTP body is null
	 * @param path
	 * @param query
	 * @return
	 * @throws IOException
	 */
	public Response post(List<String> path, Map<String, String> query) throws IOException {

		HttpPost httpPost = new HttpPost(getCompleteURL(path, query));	

		httpPost.addHeader("Content-Type", "application/json;charset=utf-8");
		
		httpPost.setEntity(new StringEntity("", "UTF-8"));
		
		Response response = processResponse(httpClient.execute(httpPost));	
		
		return response;
		
	}

	public Response post(String path,String headEle) throws IOException {

		String[] arr = headEle.split(",");

		HttpPost httpPost = new HttpPost(path);

		httpPost.addHeader("Content-Type", "application/json;charset=utf-8");

		httpPost.addHeader("sessionId", arr[0]);

		httpPost.addHeader("version", arr[1]);

		httpPost.setEntity(new StringEntity("", "UTF-8"));

		Response response = processResponse(httpClient.execute(httpPost));

		return response;

	}

	/**
	 * url转码（url中不允许出现特殊字符）
	 * @param url
	 * @param params
	 * @param timeout
	 * @return
	 */
	public static Response
	postHandle(String url, Map<String, String> params, int timeout) {
		HttpClient httpClient = null;
		try {
			httpClient = new DefaultHttpClient();
			initHttpClient(httpClient, timeout);
			HttpPost post = new HttpPost(url);
			post.addHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
			post.addHeader("version", "sso");
			post.addHeader("isTest", "true");
			post.addHeader("sessionId","xxxx");
//			post.addHeader("sessionId", params.get("sessionId"));
			if (params != null && !params.isEmpty()) {
				List<BasicNameValuePair> pairs = new ArrayList<BasicNameValuePair>();
				for (Map.Entry<String, String> entry : params.entrySet()) {
					pairs.add(new BasicNameValuePair(entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString()));
				}
				if (!pairs.isEmpty()) {
					post.setEntity(new UrlEncodedFormEntity(pairs, HTTP.UTF_8));
				}
			}
			Response response = processResponse(httpClient.execute(post));
			return response;

		} catch (Exception e) {
			String msg = "网络出错, 可能的原因是: 您的网络不通, 或者服务器停掉了! url:" + url;
			if (params != null && params.get("method") != null) {
				msg += ", method:" + params.get("method");
			}
			logger.error(msg + "," + e);
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return null;
	}

	private static void initHttpClient(HttpClient httpClient, int timeout) {
		httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, timeout);
		httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,2000);
	}

	public Response postForNOS(List<String> path, Map<String, String> query, String nosAuth, String body) throws IOException {

		HttpPost httpPost = new HttpPost(getCompleteURL(path, query));	
//		httpPost.addHeader("Host", "106.2.124.109");
		httpPost.addHeader("Content-Type", "application/octet-stream");
		httpPost.addHeader("Authorization", nosAuth);
		httpPost.setEntity(new StringEntity(body, "UTF-8"));
 
		Response response = processResponse(httpClient.execute(httpPost));	
		
		return response;
		
	}
	
	// for NOS upload
	public Response post(List<String> path, Map<String, String> query, Map<String, String> httpHeader, String httpBody) throws IOException {

		HttpPost httpPost = new HttpPost(getCompleteURL(path, query));	
		
		if( httpHeader == null || httpHeader.isEmpty()){
			logger.error("http header is invalid");
			return null;
		}
		if(httpBody == null){
			logger.error("http body is null which is invalid");
			return null;
		}
		
		for(Map.Entry<String, String> entry:httpHeader.entrySet())
			httpPost.addHeader(entry.getKey(), entry.getValue());
		
		httpPost.addHeader("Content-Type", "application/json;charset=utf-8");
		httpPost.setEntity(new StringEntity(httpBody, "UTF-8"));	
		
		logger.info("http header is : " + httpHeader);
		logger.info("http body is : " + httpBody);
		
		Response response = processResponse(httpClient.execute(httpPost));	
		
		return response;
		
	}
	
	public Response post(List<String> path, Map<String, String> query, String httpBody) throws IOException {

		HttpPost httpPost = new HttpPost(getCompleteURL(path, query));	
		
		httpPost.addHeader("Content-Type", "application/json;charset=utf-8");
		httpPost.setEntity(new StringEntity(httpBody, "UTF-8"));	
		logger.info("the http body is: " + httpBody);
		Response response = processResponse(httpClient.execute(httpPost));	
		
		return response;
		
	}
	
	
	public Response get(List<String> path, Map<String, String> query) throws IOException {

		HttpGet httpGet = new HttpGet(getCompleteURL(path, query));	
		httpGet.addHeader("Content-Type", "application/json;charset=utf-8");
		Response response = processResponse(httpClient.execute(httpGet));	
		
		return response;
		
	}
	
	/**
	 * add user-agent to the http header
	 * @param path
	 * @param query
	 * @param user_agent
	 * @return
	 * @throws IOException
	 */
	public Response get(List<String> path, Map<String, String> query, String user_agent) throws IOException {

		HttpGet httpGet = new HttpGet(getCompleteURL(path, query));	
		httpGet.addHeader("Content-Type", "application/json;charset=utf-8");
		httpGet.addHeader("User-Agent", user_agent);
		Response response = processResponse(httpClient.execute(httpGet));	
		
		return response;
		
	}
	
	public Response delete(List<String> path, Map<String, String> query, HttpContent httpContent) throws IOException {

		HttpDelete httpDelete = new HttpDelete(getCompleteURL(path, query));	
		
		if( !httpContent.httpHeaderIsValid() ){
			logger.error("http header is invalid");
			return null;
		}
		for(Map.Entry<String, String> entry:httpContent.getHttpHeader().entrySet())
			httpDelete.addHeader(entry.getKey(), entry.getValue());
		
		httpDelete.addHeader("Content-Type", "application/json;charset=utf-8");
		
		Response response = processResponse(httpClient.execute(httpDelete));	
		
		return response;
		
	}


}
