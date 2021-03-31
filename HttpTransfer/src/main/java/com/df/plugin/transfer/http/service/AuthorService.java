package com.df.plugin.transfer.http.service;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collections;
import java.util.regex.Pattern;

import com.df.plugin.transfer.http.config.HttpConfig;
import com.github.lixiang2114.netty.scope.HttpServletRequest;
import com.github.lixiang2114.netty.scope.HttpServletResponse;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author Lixiang
 * @description 认证服务
 */
public class AuthorService {
	/**
	 * Http服务配置
	 */
	private HttpConfig httpConfig;
	
	/**
	 * 基础表单认证
	 * 流行的浏览器都支持BASE认证模式
	 */
	private static final String BASE_REALM="Basic Realm=\"test\"";
	
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
	/**
	 * 空白正则式
	 */
	private static final Pattern BLANK_REGEX=Pattern.compile("\\s+");
	
	public AuthorService(HttpConfig httpConfig) {
		this.httpConfig=httpConfig;
	}
	
	/**
	 * 普通认证方式(查询字串认证)
	 * @param request 请求对象
	 * @return 是否认证成功
	 */
	public Boolean queryAuthor(HttpServletRequest request,HttpServletResponse response) {
		if(null==httpConfig.userField || null==httpConfig.passField) return null;
		String userName=request.getParameter(httpConfig.userField);
		String passWord=request.getParameter(httpConfig.passField);
		if(httpConfig.userName.equalsIgnoreCase(userName) && httpConfig.passWord.equalsIgnoreCase(passWord)) {
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
		if(null==authorization || authorization.trim().isEmpty()){
			response.setHeader(HttpHeaderNames.WWW_AUTHENTICATE.toString(), BASE_REALM);
			response.setStatus(HttpResponseStatus.UNAUTHORIZED);
			return null;
		}else{
			String[] authorArray=BLANK_REGEX.split(authorization);
			if(2>authorArray.length) {
				return false;
			}else{
				String encodedBase64=authorArray[1].trim();
				if(0==encodedBase64.length()) return false;
				String decodedBase64=new String(Base64.getDecoder().decode(encodedBase64),Charset.forName("UTF-8"));
				String[] userAndPass=COLON_REGEX.split(decodedBase64);
				if(2>userAndPass.length) {
					return false;
				}else{
					String userName=userAndPass[0].trim();
					String passWord=userAndPass[1].trim();
					if(httpConfig.userName.equalsIgnoreCase(userName) && httpConfig.passWord.equalsIgnoreCase(passWord)) {
						request.setAttribute("loginUser", Collections.singletonMap(userName, passWord));
						return true;
					}else{
						return false;
					}
				}
			}
		}
	}
}
