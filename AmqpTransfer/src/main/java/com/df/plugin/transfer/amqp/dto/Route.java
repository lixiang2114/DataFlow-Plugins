package com.df.plugin.transfer.amqp.dto;

import org.springframework.amqp.core.Binding;

/**
 * @author Lixiang
 * @description 路由实体
 */
public class Route {
	/**
	 * 源组件类型
	 */
	public String srcType;
	
	/**
	 * 目标组件类型
	 */
	public String dstType;
	
	/**
	 * 源组件名称
	 */
	public String srcName;
	
	/**
	 * 目标组件名称
	 */
	public String dstName;
	
	/**
	 * 绑定对象
	 */
	public Binding binding;
	
	/**
	 * 路由键
	 */
	public String routingKey;
	
	public Route(Binding binding,String dstName,String dstType,String srcName,String srcType,String routingKey) {
		this.binding=binding;
		this.srcType=srcType;
		this.dstType=dstType;
		this.srcName=srcName;
		this.dstName=dstName;
		this.routingKey=routingKey;
	}

	@Override
	public String toString() {
		return new StringBuilder(dstName).append(":").append(dstType).append(";")
				.append(srcName).append(":").append(srcType).append(";")
				.append(routingKey)
				.toString();
	}
}
