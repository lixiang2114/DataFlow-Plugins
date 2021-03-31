package com.df.plugin.extend.filter.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.Context;
import com.github.lixiang2114.flow.util.ClassLoaderUtil;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * 扩展过滤器配置
 */
@SuppressWarnings("unchecked")
public class FilterConfig {
	/**
	 * 过滤器运行时路径
	 */
	private File filterPath;
	
	/**
	 * 是否采用透传模式
	 * true:忽略解析脚本,直接透传上游通道数据到下游通道
	 * false:默认值,执行脚本解析,并将解析结果压入下游通道
	 */
	public boolean through;
	
	/**
	 * 入口类名
	 */
	public String mainClass;
	
	/**
	 * 过滤器参数配置
	 */
	private Properties config;
	
	/**
	 * 入口函数名
	 */
	public String mainMethod;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FilterConfig.class);
	
	public FilterConfig(){}
	
	public FilterConfig(Flow flow){
		this.filterPath=flow.filterPath;
		this.config=PropertiesReader.getProperties(new File(filterPath,"filter.properties"));
	}
	
	/**
	 * 配置文件发送器
	 * @param config
	 */
	public FilterConfig config() {
		String throughStr=config.getProperty("through","").trim();
		this.through=throughStr.isEmpty()?false:Boolean.parseBoolean(throughStr);
		if(this.through) return this;
		
		File libPath=new File(filterPath,"lib");
		File binPath=new File(filterPath,"bin");
		File srcPath=new File(filterPath,"script");
		File appPath=new File(Context.projectFile,"lib");
		
		if(ClassLoaderUtil.compileJavaSource(srcPath,binPath,new File[]{appPath,libPath})) {
			ClassLoaderUtil.addFileToCurrentClassPath(binPath);
		}else{
			log.error("compile script failure: {}",srcPath.getAbsolutePath());
			throw new RuntimeException("compile script failure: "+srcPath.getAbsolutePath());
		}
		
		String mainClassStr=config.getProperty("mainClass","").trim();
		this.mainClass=mainClassStr.isEmpty()?"DefaultClass":mainClassStr;
		
		String mainMethodStr=config.getProperty("mainMethod","").trim();
		this.mainMethod=mainMethodStr.isEmpty()?"main":mainMethodStr;
		
		return this;
	}
	
	/**
	 * 获取字段值
	 * @param key 键
	 * @return 返回字段值
	 */
	public Object getFieldValue(String key) {
		if(null==key) return null;
		String attrName=key.trim();
		if(attrName.isEmpty()) return null;
		
		try {
			Field field=FilterConfig.class.getDeclaredField(attrName);
			field.setAccessible(true);
			
			Object fieldVal=field.get(this);
			Class<?> fieldType=field.getType();
			if(!fieldType.isArray()) return fieldVal;
			
			int len=Array.getLength(fieldVal);
			StringBuilder builder=new StringBuilder("[");
			for(int i=0;i<len;builder.append(Array.get(fieldVal, i++)).append(","));
			if(builder.length()>1) builder.deleteCharAt(builder.length()-1);
			builder.append("]");
			return builder.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 设置字段值
	 * @param key 键
	 * @param value 值
	 * @return 返回设置后的值
	 */
	public Object setFieldValue(String key,Object value) {
		if(null==key || null==value) return null;
		String attrName=key.trim();
		if(attrName.isEmpty()) return null;
		
		try {
			Field field=FilterConfig.class.getDeclaredField(attrName);
			field.setAccessible(true);
			
			Class<?> fieldType=field.getType();
			Object fieldVal=null;
			if(String[].class==fieldType){
				fieldVal=COMMA_REGEX.split(value.toString());
			}else if(File.class==fieldType){
				fieldVal=new File(value.toString());
			}else if(CommonUtil.isSimpleType(fieldType)){
				fieldVal=CommonUtil.transferType(value, fieldType);
			}else{
				return null;
			}
			
			field.set(this, fieldVal);
			return value;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 收集实时参数
	 * @return
	 */
	public String collectRealtimeParams() {
		HashMap<String,Object> map=new HashMap<String,Object>();
		map.put("through", through);
		map.put("filterPath", filterPath);
		map.put("mainClass", mainClass);
		map.put("mainMethod", mainMethod);
		return map.toString();
	}
}
