package com.df.plugin.sink.amqp.dto;

import java.util.Objects;

/**
 * @author Lixiang
 * @description Amqp映射器
 */
public class AmqpMapper {
	/**
	 * 交换器
	 */
	public String exchange;
	
	/**
	 * 路由键
	 */
	public String routingKey;
	
	/**
	 * 消息对象
	 */
	public String message;
	
	public AmqpMapper(String exchange,String routingKey,String message) {
		this.message=message;
		this.exchange=exchange;
		this.routingKey=routingKey;
	}
	
	@Override
	public String toString() {
		return new StringBuilder("exchange: ").append(exchange)
				.append(",routingKey: ").append(routingKey)
				.append(",message: ").append(message)
				.toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(exchange,routingKey,message);
	}

	@Override
	public boolean equals(Object obj) {
		if(this==obj) return true;
		if(!(obj instanceof AmqpMapper)) return false;
		AmqpMapper amqpMapper = (AmqpMapper)obj;
		if(null==routingKey) return (exchange.equals(amqpMapper.exchange) && message.equals(amqpMapper.message))?true:false;
		return (exchange.equals(amqpMapper.exchange) && routingKey.equals(amqpMapper.routingKey) && message.equals(amqpMapper.message))?true:false;
	}
}
