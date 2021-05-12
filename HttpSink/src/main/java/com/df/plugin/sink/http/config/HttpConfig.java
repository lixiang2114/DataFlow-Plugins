package com.df.plugin.sink.http.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.SizeUnit;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description Http发送器配置
 */
@SuppressWarnings("unchecked")
public class HttpConfig {
	/**
	 * 流程对象
	 */
	public Flow flow;
	
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
	 * 缓冲文件已经读取的行数
	 */
	public int lineNumber;
	
	/**
	 * 缓冲文件已经读取的字节数量
	 */
	public long byteNumber;
	
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
	 * 转存文件
	 */
	public File transferSaveFile;
	
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
	 * 缓冲日志文件被读完后自动删除
	 */
	public Boolean delOnReaded;
	
	/**
	 * 批量等待最大时间(毫秒)
	 */
	public Long maxBatchWaitMills;
	
	/**
	 * 发送失败后最大重试次数
	 */
	public Integer maxRetryTimes;
	
	/**
	 * 转存文件最大尺寸
	 */
	public Long transferSaveMaxSize;
	
	/**
	 * 默认转存文件
	 */
	public File defaultTransferSaveFile;
	
	/**
	 * 是否在登录成功之后立即推送数据
	 * true:登录成功之后立即推送数据
	 * false:登录成功之后再次请求才推送数据
	 */
	public boolean pushOnLoginSuccess;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HttpConfig.class);
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	public HttpConfig(){}
	
	public HttpConfig(Flow flow){
		this.flow=flow;
		this.sinkPath=flow.sinkPath;
		this.defaultTransferSaveFile=new File(sinkPath,"tmp/buffer.log.0");
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
		
		String pushOnLoginSuccessStr=config.getProperty("pushOnLoginSuccess","").trim();
		this.pushOnLoginSuccess=pushOnLoginSuccessStr.isEmpty()?false:Boolean.parseBoolean(pushOnLoginSuccessStr);
		
		String maxRetryTimesStr=config.getProperty("maxRetryTimes","").trim();
		this.maxRetryTimes=maxRetryTimesStr.isEmpty()?Integer.MAX_VALUE:Integer.parseInt(maxRetryTimesStr);
		
		String failMaxWaitMillStr=config.getProperty("failMaxWaitMills","").trim();
		this.failMaxWaitMills=failMaxWaitMillStr.isEmpty()?0L:Long.parseLong(failMaxWaitMillStr);
		
		String sendTypeStr=config.getProperty("sendType","").trim();
		this.sendType=sendTypeStr.isEmpty()?RecvType.StreamBody:RecvType.valueOf(sendTypeStr);
		
		String requireLoginStr=config.getProperty("requireLogin","").trim();
		this.requireLogin=requireLoginStr.isEmpty()?true:Boolean.parseBoolean(requireLoginStr);
		
		String loginURLStr=config.getProperty("loginURL","").trim();
		this.loginURL=loginURLStr.isEmpty()?null:loginURLStr;
		
		String userFieldStr=config.getProperty("userField","").trim();
		this.userField=userFieldStr.isEmpty()?null:userFieldStr;
		
		String passFieldStr=config.getProperty("passField","").trim();
		this.passField=passFieldStr.isEmpty()?null:passFieldStr;
		
		String authorModeStr=config.getProperty("authorMode","").trim();
		this.authorMode=authorModeStr.isEmpty()?"base":authorModeStr;
		
		String userNameStr=config.getProperty("userName","").trim();
		this.userName=userNameStr.isEmpty()?null:userNameStr;
		
		String passWordStr=config.getProperty("passWord","").trim();
		this.passWord=passWordStr.isEmpty()?null:passWordStr;
		
		String messageFieldStr=config.getProperty("messageField","").trim();
		this.messageField=messageFieldStr.isEmpty()?null:messageFieldStr;
		if(null!=this.messageField) {
			if(RecvType.StreamBody==sendType || RecvType.MessageBody==sendType) {
				log.error("when messageField is not empty,sendType must not be StreamBody or MessageBody...");
				throw new RuntimeException("when messageField is not empty,sendType must not be StreamBody or MessageBody...");
			}
		}
		
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
				
				if(RecvType.StreamBody==sendType || RecvType.MessageBody==sendType) {
					log.error("when authorMode=query,sendType must not be StreamBody or MessageBody...");
					throw new RuntimeException("when authorMode=query,sendType must not be StreamBody or MessageBody...");
				}
			}
		}
		
		String transferSaveFileName=config.getProperty("transferSaveFile","").trim();
		this.transferSaveFile=transferSaveFileName.isEmpty()?defaultTransferSaveFile:new File(transferSaveFileName);
		
		String maxBatchWaitMillStr=config.getProperty("maxBatchWaitMills","").trim();
		this.maxBatchWaitMills=maxBatchWaitMillStr.isEmpty()?15000L:Long.parseLong(maxBatchWaitMillStr);
		
		File transferDir=transferSaveFile.getParentFile();
		if(!transferDir.exists()) transferDir.mkdirs();
		log.info("transfer save file is: "+transferSaveFile.getAbsolutePath());
		
		transferSaveMaxSize=getTransferSaveMaxSize();
		log.info("transfer save file max size is: "+transferSaveMaxSize);
		
		String delOnReadedStr=config.getProperty("delOnReaded","").trim();
		this.delOnReaded=delOnReadedStr.isEmpty()?true:Boolean.parseBoolean(delOnReadedStr);
		
		String lineNumberStr=config.getProperty("lineNumber","").trim();
		this.lineNumber=lineNumberStr.isEmpty()?0:Integer.parseInt(lineNumberStr);
		
		String byteNumberStr=config.getProperty("byteNumber","").trim();
		this.byteNumber=byteNumberStr.isEmpty()?0:Integer.parseInt(byteNumberStr);
		
		log.info("lineNumber is: "+lineNumber+",byteNumber is: "+byteNumber);
		
		return this;
	}
	
	/**
	 * 获取转存文件最大尺寸(默认为2GB)
	 */
	private Long getTransferSaveMaxSize(){
		String configMaxVal=config.getProperty("transferSaveMaxSize","").trim();
		if(configMaxVal.isEmpty()) return 2*1024*1024*1024L;
		Matcher matcher=CAP_REGEX.matcher(configMaxVal);
		if(!matcher.find()) return 2*1024*1024*1024L;
		return SizeUnit.getBytes(Long.parseLong(matcher.group(1)), matcher.group(2).substring(0,1));
	}
	
	/**
	 * 刷新文件检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		config.setProperty("lineNumber",""+lineNumber);
		config.setProperty("byteNumber",""+byteNumber);
		config.setProperty("transferSaveFile", transferSaveFile.getAbsolutePath());
		log.info("reflesh checkpoint...");
		OutputStream fos=null;
		try{
			fos=new FileOutputStream(new File(sinkPath,"sink.properties"));
			config.store(fos, "reflesh checkpoint");
		}finally{
			if(null!=fos) fos.close();
		}
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
		map.put("lineNumber", lineNumber);
		map.put("authorMode", authorMode);
		map.put("requireLogin", requireLogin);
		map.put("byteNumber", byteNumber);
		map.put("messageField", messageField);
		map.put("delOnReaded", delOnReaded);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("transferSaveFile", transferSaveFile);
		map.put("failMaxWaitMills", failMaxWaitMills);
		map.put("maxBatchWaitMills", maxBatchWaitMills);
		map.put("transferSaveMaxSize", transferSaveMaxSize);
		map.put("pushOnLoginSuccess", pushOnLoginSuccess);
		map.put("defaultTransferSaveFile", defaultTransferSaveFile);
		return map.toString();
	}
}
