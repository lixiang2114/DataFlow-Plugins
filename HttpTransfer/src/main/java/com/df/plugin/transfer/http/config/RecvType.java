package com.df.plugin.transfer.http.config;

/**
 * @author Lixiang
 * @description 协议接收类型
 */
public enum RecvType {
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
	
	private RecvType(String typeName){
		this.typeName=typeName;
	}
}
