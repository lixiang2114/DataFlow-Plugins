package com.df.plugin.sink.redis.dto;

import java.util.Objects;

import com.df.plugin.sink.redis.config.RedisType;

/**
 * @author Lixiang
 * @description Redis映射器
 */
public class RedisMapper {
	/**
	 * 目标对象
	 */
	public String target;
	
	/**
	 * 消息对象
	 */
	public Object message;
	
	/**
	 * 目标类型
	 */
	public RedisType targetType;
	
	public RedisMapper(String target,Object message,RedisType targetType) {
		this.target=target;
		this.message=message;
		this.targetType=targetType;
	}
	
	@Override
	public String toString() {
		return new StringBuilder("target: ").append(target)
				.append(",targetType: ").append(targetType)
				.append(",message: ").append(message)
				.toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(target,message,targetType);
	}

	@Override
	public boolean equals(Object obj) {
		if(this==obj) return true;
		if(!(obj instanceof RedisMapper)) return false;
		RedisMapper redisMapper = (RedisMapper)obj;
		return (target.equals(redisMapper.target) && message.equals(redisMapper.message))?true:false;
	}
}
