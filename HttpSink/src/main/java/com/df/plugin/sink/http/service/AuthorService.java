package com.df.plugin.sink.http.service;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashMap;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.util.LinkedMultiValueMap;

import com.df.plugin.sink.http.config.HttpConfig;
import com.github.lixiang2114.flow.util.RestClient;
import com.github.lixiang2114.flow.util.RestClient.WebResponse;

/**
 * @author Lixiang
 * @description 认证服务组件
 */
public class AuthorService {
	/**
	 * Http客户端配置
	 */
	private HttpConfig httpConfig;

	public AuthorService(HttpConfig httpConfig) {
		this.httpConfig=httpConfig;
	}
	
	/**
	 * 获取BASE认证信息
	 * @param message 消息
	 */
	public String getBaseRealm() {
		String realm=new StringBuilder(httpConfig.userName).append(":").append(httpConfig.passWord).toString();
		String encodedBase64=Base64.getEncoder().encodeToString(realm.getBytes(Charset.forName("UTF-8")));
		return new StringBuilder("Basic ").append(encodedBase64).toString();
	}
	
	/**
	 * 查询字串认证模式
	 * @return 是否登录成功
	 */
	public boolean queryAuthor(HashMap<String,Object> respMap) {
		LinkedMultiValueMap<String,Object> httpBody=new LinkedMultiValueMap<String,Object>();
		httpBody.add(httpConfig.userField, httpConfig.userName);
		httpBody.add(httpConfig.passField, httpConfig.passWord);
		return queryAuthor(respMap,httpBody);
	}
	
	/**
	 * 查询字串认证模式
	 * @return 是否登录成功
	 */
	public boolean queryAuthor(HashMap<String,Object> respMap,LinkedMultiValueMap<String,Object> httpBody) {
		WebResponse<String> webResponse=RestClient.post(httpConfig.loginURL, httpBody,new Object[0]);
		Integer statusCode=webResponse.getStatusCode();
		String respMsg=webResponse.getBody();
		HttpStatus okStatus=HttpStatus.OK;
		
		respMap.put("statusCode", statusCode);
		respMap.put("respMsg", respMsg);
		
		return okStatus.value()==statusCode && okStatus.getReasonPhrase().equalsIgnoreCase(respMsg);
	}
	
	/**
	 * 查询字串认证模式
	 * @return 是否登录成功
	 */
	public boolean queryAuthor(LinkedMultiValueMap<String,Object> httpBody) {
		WebResponse<String> webResponse=RestClient.post(httpConfig.loginURL, httpBody,new Object[0]);
		Integer statusCode=webResponse.getStatusCode();
		String respMsg=webResponse.getBody();
		HttpStatus okStatus=HttpStatus.OK;
		return okStatus.value()==statusCode && okStatus.getReasonPhrase().equalsIgnoreCase(respMsg);
	}
	
	/**
	 * 基础头域认证模式
	 * @return 是否登录成功
	 */
	public boolean baseAuthor(HashMap<String,Object> respMap) {
		HttpHeaders httpHeader=RestClient.getDefaultRequestHeader();
		httpHeader.set(HttpHeaders.AUTHORIZATION, getBaseRealm());
		return baseAuthor(respMap,httpHeader);
	}
	
	/**
	 * 基础头域认证模式
	 * @return 是否登录成功
	 */
	public boolean baseAuthor(HashMap<String,Object> respMap,HttpHeaders httpHeader) {
		WebResponse<String> webResponse=RestClient.post(httpConfig.loginURL,httpHeader,new Object[0]);
		Integer statusCode=webResponse.getStatusCode();
		String respMsg=webResponse.getBody();
		HttpStatus okStatus=HttpStatus.OK;
		
		respMap.put("statusCode", statusCode);
		respMap.put("respMsg", respMsg);
		
		return okStatus.value()==statusCode && okStatus.getReasonPhrase().equalsIgnoreCase(respMsg);
	}
	
	/**
	 * 基础头域认证模式
	 * @return 是否登录成功
	 */
	public boolean baseAuthor(HttpHeaders httpHeader) {
		WebResponse<String> webResponse=RestClient.post(httpConfig.loginURL,httpHeader,new Object[0]);
		Integer statusCode=webResponse.getStatusCode();
		String respMsg=webResponse.getBody();
		HttpStatus okStatus=HttpStatus.OK;
		return okStatus.value()==statusCode && okStatus.getReasonPhrase().equalsIgnoreCase(respMsg);
	}
}
