package com.df.plugin.transfer.redis.config;

/**
 * @author Lixiang
 * @description Redis架构模型
 */
public enum RedisArch {
	/**
	 * 单点架构
	 */
	single("single"),
	
	/**
	 * 集群架构
	 */
	cluster("cluster");
	
	/**
	 * Redis架构
	 */
	public String name;
	
	private RedisArch(String name) {
		this.name=name;
	}
}
