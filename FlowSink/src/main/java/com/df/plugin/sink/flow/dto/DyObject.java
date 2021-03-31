package com.df.plugin.sink.flow.dto;

import java.lang.reflect.Method;

/**
 * @author Lixiang
 * @description 反射对象封装实体
 */
public class DyObject {
	/**
	 * 反射对象
	 */
	private Object instance;
	
	/**
	 * 反射方法
	 */
	private Method method;
	
	public DyObject(Object instance,Method method) {
		this.instance=instance;
		this.method=method;
	}
	
	public Object invoke(Object... params) throws Exception {
		return method.invoke(instance, params);
	}
}
