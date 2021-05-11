package com.df.plugin.sink.server.service;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.regex.Pattern;

/**
 * @author Lixiang
 * @description 认证
 */
public abstract class Author {
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
	/**
	 * 任何空白正则式
	 */
	private static final Pattern BLANK_REGEX=Pattern.compile("\\s+");
	
	/**
	 * 基础表单认证
	 * 流行的浏览器都支持BASE认证模式
	 */
	protected static final String BASE_REALM="Basic Realm=\"test\"";
	
	/**
	 * 解析用户名和密码认证信息
	 * @param authorization 认证信息
	 * @return 用户名和密码
	 */
	public static final String[] parseBaseAuthorInfo(String authorization) {
		if(null==authorization || (authorization=authorization.trim()).isEmpty()) return null;
		String[] authorArray=BLANK_REGEX.split(authorization);
		if(2>authorArray.length) return null;
		String encodedBase64=authorArray[1].trim();
		if(0==encodedBase64.length()) return null;
		String decodedBase64=new String(Base64.getDecoder().decode(encodedBase64),Charset.forName("UTF-8"));
		String[] userAndPass=COLON_REGEX.split(decodedBase64);
		if(2>userAndPass.length) return null;
		String userName=userAndPass[0].trim();
		String passWord=userAndPass[1].trim();
		if(userName.isEmpty() || passWord.isEmpty()) return null;
		return new String[]{userName,passWord};
	}
}
