package com.df.plugin.sink.server.config;

/**
 * @author Lixiang
 * @description 发送服务协议
 */
public enum ServerProtocol {
	/**
	 * TCP协议
	 */
	TCP("TCP"),
	
	/**
	 * HTTP协议
	 */
	HTTP("HTTP");
	
	private String typeName;
	
	public String typeName(){
		return typeName;
	}
	
	private ServerProtocol(String typeName){
		this.typeName=typeName;
	}
	
	/**
	 * 获取协议枚举
	 * @param typeName 类型名称
	 * @return 协议枚举
	 */
	public static ServerProtocol getProtocol(String typeName){
		return ServerProtocol.valueOf(typeName.toUpperCase());
	}
}
