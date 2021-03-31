package com.df.plugin.extend.filter.util;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import com.df.plugin.extend.filter.dto.DyObject;

/**
 * @author Lixiang
 * @description 动态脚本工具
 */
public class DyScriptUtil {
	/**
	 * 反射实例缓存字典
	 */
	private static final ConcurrentHashMap<String,DyObject> dyCache=new ConcurrentHashMap<String,DyObject>();
	
	/**
	 * 反射类缓存字典
	 */
	private static final ConcurrentHashMap<String,Class<?>> reflexCache=new ConcurrentHashMap<String,Class<?>>();
	
	/**
	 * 调用参数指定类中的指定函数
	 * @param classLoader 类装载器
	 * @param className 被调函数所在的类
	 * @param methodName 被调用函数方法名
	 * @param params 被调函数参数(只能传递非null值)
	 * @return 被调函数返回值
	 * @throws Exception
	 */
	public static Object execFunc(String className,String methodName,Object... params) throws Exception{
		DyObject dyObject=dyCache.get(getMethodSign(className,methodName,params));
		if(null!=dyObject) return dyObject.invoke(params);
		return execFunc(null,className,methodName,params);
	}
	
	/**
	 * 调用参数指定类中的指定函数
	 * @param classLoader 类装载器
	 * @param className 被调函数所在的类
	 * @param methodName 被调用函数方法名
	 * @param params 被调函数参数(只能传递非null值)
	 * @return 被调函数返回值
	 * @throws Exception
	 */
	public static Object execFunc(ClassLoader classLoader,String className,String methodName,Object... params) throws Exception{
		if(null==className || null==methodName) throw new RuntimeException("class and method name can not be null...");
		if((methodName=methodName.trim()).isEmpty()) throw new RuntimeException("method name can not be empty...");
		if((className=className.trim()).isEmpty()) throw new RuntimeException("class name can not be empty...");
		
		Class<?>[] argTypes=null;
		if(null==params) {
			argTypes=new Class<?>[0];
		}else{
			argTypes=new Class[params.length];
			for(int i=0;i<params.length;argTypes[i]=params[i++].getClass());
		}
		
		Class<?> type=getClass(classLoader,className);
		Method[] methods=type.getMethods();
		Method targetMethod=null;
		for(Method method:methods) {
			if(!methodName.equals(method.getName())) continue;
			Class<?>[] destTypes=method.getParameterTypes();
			if(!compatible(destTypes,argTypes)) continue;
			targetMethod=method;
			break;
		}
		
		if(null==targetMethod) throw new RuntimeException("not found method: "+methodName);
		
		DyObject dyObject=new DyObject(type.newInstance(),targetMethod);
		dyCache.put(getMethodSign(className,methodName,params),dyObject);
		return dyObject.invoke(params);
	}
	
	/**
	 * 获取反射类对象
	 * @param classLoader
	 * @param classFullName
	 * @return 反射类
	 * @throws ClassNotFoundException
	 */
	public static final Class<?> getClass(ClassLoader classLoader,String classFullName) throws ClassNotFoundException {
		if(isEmpty(classFullName)) return null;
		Class<?> type=reflexCache.get(classFullName=classFullName.trim());
		if(null==type) {
			if(null==classLoader) classLoader=Thread.currentThread().getContextClassLoader();
			reflexCache.put(classFullName, type=classLoader.loadClass(classFullName));
		}
		return type;
	}
	
	/**
	 * 判断给定的基类型superTypes数组是否可以兼容到指定的子类型数组childTypes
	 * @param superTypes 基类型
	 * @param childTypes 子类型
	 * @return 数组类型是否兼容
	 */
	public static final boolean compatible(Class<?>[] superTypes,Class<?>[] childTypes){
		if(superTypes==childTypes) return true;
		if(null==superTypes && null!=childTypes) return false;
		if(null!=superTypes && null==childTypes) return false;
		if(superTypes.length!=childTypes.length) return false;
		try{
			for(int i=0;i<superTypes.length;i++){
				if(superTypes[i]==childTypes[i] || superTypes[i].isAssignableFrom(childTypes[i])) continue;
				if(superTypes[i].isPrimitive() && superTypes[i]==childTypes[i].getField("TYPE").get(null)) continue;
				if(childTypes[i].isPrimitive() && childTypes[i]==superTypes[i].getField("TYPE").get(null)) continue;
				return false;
			}
			return true;
		}catch(Exception e){
			return false;
		}
	}
	
	/**
	 * 获取方法签名段
	 * @param methodDeclare 方法声明
	 * @return
	 */
	public static String getMethodSign(String className,String methodName,Object... params) {
		StringBuilder builder=new StringBuilder(className).append("+").append(methodName);
		if(null!=params) {
			int len=params.length;
			for(int i=0;i<len;builder.append("+").append(params[i++].getClass().getSimpleName()));
		}
		return builder.toString();
	}
	
	/**
	 * 字串是否为空
	 * @param value 字串
	 * @return 是否为空
	 */
	public static final boolean isEmpty(String value) {
		if(null==value) return true;
		return value.trim().isEmpty();
	}
}
