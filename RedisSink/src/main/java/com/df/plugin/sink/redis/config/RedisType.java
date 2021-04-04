package com.df.plugin.sink.redis.config;

/**
 * @author Lixiang
 * @description Redis结构类型
 */
public enum RedisType {
	/**
	 * 字典结构
	 * 适用于分布式缓存
	 */
	dict("dict"),
	
	/**
	 * 管道结构
	 * 适用于消息中间件
	 */
	pipe("pipe");
	
	/**
	 * 数据结构类型名称
	 */
	public String name;
	
	private RedisType(String name) {
		this.name=name;
	}
}
