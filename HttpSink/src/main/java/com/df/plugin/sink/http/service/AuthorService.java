package com.df.plugin.sink.http.service;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashMap;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;

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
	 * 查询字串认证模式
	 * @return 是否登录成功
	 */
	public boolean queryAuthor(HashMap<String,Object> respMap) {
		HashMap<String,Object> httpBody=new HashMap<String,Object>();
		httpBody.put(httpConfig.userField, httpConfig.userName);
		httpBody.put(httpConfig.passField, httpConfig.passWord);
		
		WebResponse<String> webResponse=RestClient.post(httpConfig.loginURL, httpBody,new Object[0]);
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
	public boolean baseAuthor(HashMap<String,Object> respMap) {
		String realm=new StringBuilder(httpConfig.userName).append(":").append(httpConfig.passWord).toString();
		String encodedBase64=Base64.getEncoder().encodeToString(realm.getBytes(Charset.forName("UTF-8")));
		String baseRealm=new StringBuilder("Basic ").append(encodedBase64).toString();
		
		HttpHeaders httpHeader=RestClient.getDefaultRequestHeader();
		httpHeader.set(HttpHeaders.AUTHORIZATION, baseRealm);
		
		WebResponse<String> webResponse=RestClient.post(httpConfig.loginURL,httpHeader,new Object[0]);
		Integer statusCode=webResponse.getStatusCode();
		String respMsg=webResponse.getBody();
		HttpStatus okStatus=HttpStatus.OK;
		
		respMap.put("statusCode", statusCode);
		respMap.put("respMsg", respMsg);
		
		return okStatus.value()==statusCode && okStatus.getReasonPhrase().equalsIgnoreCase(respMsg);
	}
}
