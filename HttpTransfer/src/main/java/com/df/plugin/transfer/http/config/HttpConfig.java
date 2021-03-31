package com.df.plugin.transfer.http.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.SizeUnit;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description Http转存器配置
 */
@SuppressWarnings("unchecked")
public class HttpConfig {
	/**
	 * Web服务端口
	 */
	public int port;
	
	/**
	 * 插件运行时路径
	 */
	public File pluginPath;
	
	/**
	 * 转存目录
	 */
	public File transferPath;
	
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
	 * query:查询字串模式
	 * base:基础认证模式
	 * auto:先使用查询字串,再使用基础认证模式
	 */
	public String authorMode;
	
	/**
	 * Http客户端配置
	 */
	private Properties config;
	
	/**
	 * 协议接收类型
	 */
	public RecvType recvType;
	
	/**
	 * 错误回应
	 */
	public String errorReply;
	
	/**
	 * 正常回应
	 */
	public String normalReply;
	
	/**
	 * 登录失败标识
	 */
	public String loginFailureId;
	
	/**
	 * 登录成功标识
	 */
	public String loginSuccessId;
	
	/**
	 * 实时转存的日志文件
	 */
	public File transferSaveFile;
	
	/**
	 * 是否需要登录
	 */
	public boolean requireLogin;
	
	/**
	 * 转存日志文件最大尺寸
	 */
	public Long transferSaveMaxSize;
	
	/**
	 * 转存ETL通道
	 */
	public Channel<String> etlChannel;
	
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
		this.transferPath=flow.sharePath;
		this.pluginPath=flow.transferPath;
		this.config=PropertiesReader.getProperties(new File(pluginPath,"transfer.properties"));
	}
	
	/**
	 * 配置Http发送器
	 * @param config
	 */
	public HttpConfig config() {
		String userFieldStr=config.getProperty("userField","").trim();
		this.userField=userFieldStr.isEmpty()?null:userFieldStr;
		
		String passFieldStr=config.getProperty("passField","").trim();
		this.passField=passFieldStr.isEmpty()?null:passFieldStr;
		
		String passWordStr=config.getProperty("passWord","").trim();
		this.passWord=passWordStr.isEmpty()?null:passWordStr;
		
		String userNameStr=config.getProperty("userName","").trim();
		this.userName=userNameStr.isEmpty()?null:userNameStr;
		
		String authorModeStr=config.getProperty("authorMode","").trim();
		this.authorMode=authorModeStr.isEmpty()?"auto":authorModeStr;
		
		String requireLoginStr=config.getProperty("requireLogin","").trim();
		this.requireLogin=requireLoginStr.isEmpty()?true:Boolean.parseBoolean(requireLoginStr);
		
		if(requireLogin) {
			if(null==userName || null==passWord) {
				log.error("userName and passWord must be exists when requireLogin is true...");
				throw new RuntimeException("userName and passWord must be exists when requireLogin is true...");
			}
			
			if("auto".equals(authorMode) || "query".equals(authorMode)) {
				if(null==userField || null==passField) {
					log.error("userField and passField must be exists when authorMode is auto or query...");
					throw new RuntimeException("userField and passField must be exists when authorMode is auto or query...");
				}
			}
		}
		
		//转存日志文件
		String transferSaveFileName=config.getProperty("transferSaveFile","").trim();
		if(transferSaveFileName.isEmpty()) {
			this.transferSaveFile=new File(transferPath,"buffer.log.0");
			log.warn("not found parameter: 'transferSaveFile',will be use default...");
		}else{
			File file=new File(transferSaveFileName);
			if(!file.exists() || file.isFile()) transferSaveFile=file;
		}
		
		if(null==transferSaveFile) {
			log.error("transferSaveFile can not be NULL...");
			throw new RuntimeException("transferSaveFile can not be NULL...");
		}
		
		log.info("transfer save file is: "+transferSaveFile.getAbsolutePath());
		
		//转存日志文件最大尺寸
		transferSaveMaxSize=getTransferSaveMaxSize();
		log.info("transfer save file max size is: "+transferSaveMaxSize);
		
		//常规参数
		String recvTypeStr=config.getProperty("recvType","").trim();
		this.recvType=recvTypeStr.isEmpty()?RecvType.MessageBody:RecvType.valueOf(recvTypeStr);
		
		String loginSuccessIdStr=config.getProperty("loginSuccessId","").trim();
		this.loginSuccessId=loginSuccessIdStr.isEmpty()?"OK":loginSuccessIdStr;
		
		String loginFailureIdStr=config.getProperty("loginFailureId","").trim();
		this.loginFailureId=loginFailureIdStr.isEmpty()?"NO":loginFailureIdStr;
		
		String normalReplyStr=config.getProperty("normalReply","").trim();
		this.normalReply=normalReplyStr.isEmpty()?"OK":normalReplyStr;
		
		String errorReplyStr=config.getProperty("errorReply","").trim();
		this.errorReply=errorReplyStr.isEmpty()?"NO":errorReplyStr;
		
		String portStr=config.getProperty("port","").trim();
		this.port=portStr.isEmpty()?8080:Integer.parseInt(portStr);
		
		return this;
	}
	
	/**
	 * 获取转存日志文件最大尺寸(默认为2GB)
	 */
	private Long getTransferSaveMaxSize(){
		String configMaxVal=config.getProperty("transferSaveMaxSize","").trim();
		if(configMaxVal.isEmpty()) return 2*1024*1024*1024L;
		Matcher matcher=CAP_REGEX.matcher(configMaxVal);
		if(!matcher.find()) return 2*1024*1024*1024L;
		return SizeUnit.getBytes(Long.parseLong(matcher.group(1)), matcher.group(2).substring(0,1));
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
		map.put("port", port);
		map.put("recvType", recvType);
		map.put("userField", userField);
		map.put("passField", passField);
		map.put("passWord", passWord);
		map.put("userName", userName);
		map.put("transferPath", transferPath);
		map.put("authorMode", authorMode);
		map.put("requireLogin", requireLogin);
		map.put("loginFailureId", loginFailureId);
		map.put("loginSuccessId", loginSuccessId);
		map.put("transferSaveFile", transferSaveFile);
		map.put("transferSaveMaxSize", transferSaveMaxSize);
		return map.toString();
	}
}
