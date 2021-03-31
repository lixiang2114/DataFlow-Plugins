package com.df.plugin.sink.http.service;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;

import com.df.plugin.sink.http.config.HttpConfig;
import com.df.plugin.sink.http.config.RecvType;
import com.github.lixiang2114.flow.util.RestClient;
import com.github.lixiang2114.flow.util.RestClient.WebResponse;

/**
 * @author Lixiang
 * @description Http服务模块
 */
public class HttpService {
	/**
	 * Http发送器配置
	 */
	private HttpConfig httpConfig;
	
	/**
	 * Http认证服务组件
	 */
	private AuthorService authorService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HttpService.class);
	
	public HttpService(){}
	
	public HttpService(HttpConfig httpConfig){
		this.httpConfig=httpConfig;
		this.authorService=new AuthorService(httpConfig);
	}
	
	/**
	 * 登录Http服务器
	 * @return 是否登录成功
	 */
	public boolean login(){
		if(!httpConfig.requireLogin) return true;
		
		boolean flag=false;
		HashMap<String,Object> respMap=new HashMap<String,Object>();
		
		switch(httpConfig.authorMode) {
			case "query":
				flag=authorService.queryAuthor(respMap);
				break;
			case "base":
				flag=authorService.baseAuthor(respMap);
				break;
			default:
				flag=false;
		}
		
		if(flag) return true;
		
		log.error("HttpSink login failure,response status code: {},response message: {}...",respMap.remove("statusCode"),respMap.remove("respMsg"));
		return false;
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws InterruptedException
	 */
	public boolean prePost() throws Exception {
		if(0==httpConfig.preFailSinkSet.size())  return true;
		for(Object object:httpConfig.preFailSinkSet){
			if(!post((String)object)) return false;
			httpConfig.preFailSinkSet.remove(object);
		}
		
		return true;
	}
	
	/**
	 * 发送消息到Http服务器
	 * @param message 消息参数
	 * @return 是否发送成功
	 * @throws Exception
	 */
	public boolean post(String message) throws Exception {
		RecvType sendType=httpConfig.sendType;
		HttpHeaders httpHeaders=RestClient.getDefaultRequestHeader();
		switch(sendType){
			case MessageBody:
				httpHeaders.setContentType(MediaType.APPLICATION_JSON_UTF8);
				break;
			case StreamBody:
				httpHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
				break;
			case QueryString:
			case ParamMap:
				httpHeaders.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
				break;
			default:
				log.info("Error: unknow sendType!");
		}
		
		LinkedMultiValueMap<String,Object> httpBody=null;
		if(null!=httpConfig.messageField) {
			httpBody=new LinkedMultiValueMap<String,Object>();
			httpBody.add(httpConfig.messageField, message);
		}
		
		boolean loop=false;
		int times=0;
		do{
			try{
				WebResponse<String> webResponse=null;
				if(null==httpBody){
					webResponse=RestClient.post(httpConfig.postURL,httpHeaders,message,new Object[0]);
				}else{
					webResponse=RestClient.post(httpConfig.postURL,httpHeaders,httpBody,new Object[0]);
				}
				
				if(null==webResponse) throw new RuntimeException("HttpSink send message failure,webResponse is NULL...");
				if(HttpStatus.OK.value()!=webResponse.getStatusCode()) throw new RuntimeException("HttpSink send message failure,status code is: "+webResponse.getStatusCode());
				
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(httpConfig.failMaxWaitMills);
				log.error("send occur excepton: "+e.getMessage());
			}
		}while(loop && times<httpConfig.maxRetryTimes);
		
		if(loop) httpConfig.preFailSinkSet.add(message);
		return !loop;
	}
}
