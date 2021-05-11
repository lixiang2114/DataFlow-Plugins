package com.df.plugin.sink.server.service;

import java.util.Collections;

import com.df.plugin.sink.server.config.ContextConfig;
import com.github.lixiang2114.netty.scope.HttpServletRequest;
import com.github.lixiang2114.netty.scope.HttpServletResponse;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author Lixiang
 * @description HTTP发送服务认证器
 */
public class HttpAuthor extends Author{
	/**
	 * 上下文配置
	 */
	private ContextConfig config;
	
	public HttpAuthor(ContextConfig config) {
		this.config=config;
	}
	
	/**
	 * 普通认证方式(查询字串认证)
	 * @param request 请求对象
	 * @return 是否认证成功
	 */
	public Boolean queryAuthor(HttpServletRequest request,HttpServletResponse response) {
		if(null==config.userField || null==config.passField) return null;
		String userName=request.getParameter(config.userField);
		String passWord=request.getParameter(config.passField);
		if(config.userName.equalsIgnoreCase(userName) && config.passWord.equalsIgnoreCase(passWord)) {
			request.setAttribute("loginUser", Collections.singletonMap(userName, passWord));
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * 基础认证方式(头域authorization认证)
	 * @param request 请求对象
	 * @return 是否认证成功
	 */
	public Boolean baseAuthor(HttpServletRequest request,HttpServletResponse response) {
		String authorization=request.getHeader(HttpHeaderNames.AUTHORIZATION.toString());
		String[] userAndPass=super.parseBaseAuthorInfo(authorization);
		
		if(null==userAndPass || 2>userAndPass.length){
			response.setHeader(HttpHeaderNames.WWW_AUTHENTICATE.toString(), BASE_REALM);
			response.setStatus(HttpResponseStatus.UNAUTHORIZED);
			return null;
		}
		
		String userName=userAndPass[0];
		String passWord=userAndPass[1];
		if(config.userName.equalsIgnoreCase(userName) && config.passWord.equalsIgnoreCase(passWord)) {
			request.setAttribute("loginUser", Collections.singletonMap(userName, passWord));
			return true;
		}else{
			return false;
		}
	}
}
