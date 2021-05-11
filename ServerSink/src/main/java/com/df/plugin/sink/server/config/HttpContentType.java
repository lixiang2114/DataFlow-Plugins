package com.df.plugin.sink.server.config;

/**
 * @author Lixiang
 * @description 发送HTTP服务内容类型
 */
public enum HttpContentType {
	/**
	 * 参数字典
	 * 适合于MIME(application/x-www-form-urlencoded)
	 */
	ParamMap("ParamMap"),
	
	/**
	 * 查询字串
	 * 适合于MIME(application/x-www-form-urlencoded)
	 */
	QueryString("QueryString"),
	
	/**
	 * 消息实体
	 * 适合于MIME(application/octet-stream)
	 */
	StreamBody("StreamBody"),

	/**
	 * 消息实体
	 * 适合于MIME(application/json)
	 */
	MessageBody("MessageBody");
	
	private String typeName;
	
	public String typeName(){
		return typeName;
	}
	
	private HttpContentType(String typeName){
		this.typeName=typeName;
	}
}
