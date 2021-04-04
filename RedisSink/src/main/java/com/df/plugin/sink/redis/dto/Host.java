package com.df.plugin.sink.redis.dto;

/**
 * @author Lixiang
 * @description 主机
 */
public class Host {
	/**
	 * IP或域名
	 */
	public String host;
	
	/**
	 * 主机端口
	 */
	public Integer port;
	
	public Host(String host,Integer port) {
		this.host=host;
		this.port=port;
	}

	@Override
	public String toString() {
		return host+":"+port;
	}
}
