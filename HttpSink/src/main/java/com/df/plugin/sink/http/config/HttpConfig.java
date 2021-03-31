package com.df.plugin.sink.http.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description Http发送器配置
 */
@SuppressWarnings("unchecked")
public class HttpConfig {
	/**
	 * 插件实例运行时路径
	 */
	public File sinkPath;
	
	/**
	 * 推送数据URL地址
	 */
	public String postURL;
	
	/**
	 * 用户登录URL地址
	 */
	public String loginURL;
	
	/**
	 * 登录用户字段名
	 */
	public String userField;
	
	/**
	 * 登录密码字段名
	 */
	public String passField;
	
	/**
	 * Http服务器登录用户
	 */
	public String userName;
	
	/**
	 * Http服务器登录密码
	 */
	public String passWord;
	
	/**
	 * 认证模式
	 * query:查询字串模式(默认)
	 * base:基础认证模式
	 */
	public String authorMode;
	
	/**
	 * Http客户端配置
	 */
	private Properties config;
	
	/**
	 * 协议发送类型
	 */
	public RecvType sendType;
	
	/**
	 * 是否需要登录
	 */
	public boolean requireLogin;
	
	/**
	 * 发送消息的字段名
	 */
	public String messageField;
	
	/**
	 * 发送失败后最大等待时间间隔
	 */
	public Long failMaxWaitMills;
	
	/**
	 * 发送失败后最大重试次数
	 */
	public Integer maxRetryTimes;
	
	/**
	 * 上次发送失败任务表
	 */
	public Set<Object> preFailSinkSet;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HttpConfig.class);
	
	public HttpConfig(){}
	
	public HttpConfig(Flow flow){
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * 配置Http发送器
	 * @param config
	 */
	public HttpConfig config() {
		String postURLStr=config.getProperty("postURL","").trim();
		if(postURLStr.isEmpty()) {
			log.error("postURL can not be empty!!!");
			throw new RuntimeException("postURL can not be empty!!!");
		}else{
			this.postURL=postURLStr;
		}
		
		String maxRetryTimesStr=config.getProperty("maxRetryTimes","").trim();
		this.maxRetryTimes=maxRetryTimesStr.isEmpty()?3:Integer.parseInt(maxRetryTimesStr);
		
		String failMaxWaitMillStr=config.getProperty("failMaxWaitMills","").trim();
		this.failMaxWaitMills=failMaxWaitMillStr.isEmpty()?2000L:Long.parseLong(failMaxWaitMillStr);
		
		String loginURLStr=config.getProperty("loginURL","").trim();
		this.loginURL=loginURLStr.isEmpty()?null:loginURLStr;
		
		String userFieldStr=config.getProperty("userField","").trim();
		this.userField=userFieldStr.isEmpty()?null:userFieldStr;
		
		String passFieldStr=config.getProperty("passField","").trim();
		this.passField=passFieldStr.isEmpty()?null:passFieldStr;
		
		String authorModeStr=config.getProperty("authorMode","").trim();
		this.authorMode=authorModeStr.isEmpty()?"query":authorModeStr;
		
		String userNameStr=config.getProperty("userName","").trim();
		this.userName=userNameStr.isEmpty()?null:userNameStr;
		
		String passWordStr=config.getProperty("passWord","").trim();
		this.passWord=passWordStr.isEmpty()?null:passWordStr;
		
		String requireLoginStr=config.getProperty("requireLogin","").trim();
		this.requireLogin=requireLoginStr.isEmpty()?true:Boolean.parseBoolean(requireLoginStr);
		
		if(requireLogin) {
			if(null==loginURL) {
				log.error("loginURL must be exists when requireLogin is true...");
				throw new RuntimeException("loginURL must be exists when requireLogin is true...");
			}
			
			if(null==userName || null==passWord) {
				log.error("userName and passWord must be exists when requireLogin is true...");
				throw new RuntimeException("userName and passWord must be exists when requireLogin is true...");
			}
			
			if("query".equals(authorMode)) {
				if(null==userField || null==passField) {
					log.error("userField and passField must be exists when authorMode is query...");
					throw new RuntimeException("userField and passField must be exists when authorMode is query...");
				}
			}
		}
		
		String sendTypeStr=config.getProperty("sendType","").trim();
		this.sendType=sendTypeStr.isEmpty()?RecvType.StreamBody:RecvType.valueOf(sendTypeStr);
		
		String messageFieldStr=config.getProperty("messageField","").trim();
		this.messageField=messageFieldStr.isEmpty()?null:messageFieldStr;
		
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
			Field field=HttpConfig.class.getDeclaredField(attrName);
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
			Field field=HttpConfig.class.getDeclaredField(attrName);
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
		map.put("postURL", postURL);
		map.put("userField", userField);
		map.put("loginURL", loginURL);
		map.put("passField", passField);
		map.put("sendType", sendType);
		map.put("passWord", passWord);
		map.put("userName", userName);
		map.put("authorMode", authorMode);
		map.put("requireLogin", requireLogin);
		map.put("messageField", messageField);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("failMaxWaitMills", failMaxWaitMills);
		return map.toString();
	}
}
