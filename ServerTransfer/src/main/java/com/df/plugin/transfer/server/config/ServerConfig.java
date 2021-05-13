package com.df.plugin.transfer.server.config;

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
 * @description 服务源配置
 */
@SuppressWarnings("unchecked")
public class ServerConfig {
	/**
	 * 扫描的目标端口
	 */
	public int port;
	
	/**
	 * 流程对象
	 */
	public Flow flow;
	
	/**
	 * 扫描的目标主机
	 */
	public String host;
	
	/**
	 * 连接字符串
	 */
	public String connStr;
	
	/**
	 * 连接协议类型
	 */
	public String protocol;
	
	/**
	 * 插件运行时路径
	 */
	public File pluginPath;
	
	/**
	 * 检查点配置
	 */
	public Properties config;
	
	/**
	 * 实时转存文件
	 */
	public File transferSaveFile;
	
	/**
	 * HTTP协议拉取间隔时间(毫秒)
	 */
	public long httpPullIntervalMills;
	
	/**
	 * 转存文件最大尺寸
	 */
	public Long transferSaveMaxSize;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ServerConfig.class);
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	/**
	 * 连接字符串正则式
	 */
	private static final Pattern CONNSTR_REGEX=Pattern.compile("(tcp|http)\\s*:\\s*/\\s*/\\s*([0-9.a-zA-Z]+)\\s*:\\s*([1-9]{1}[0-9]+)\\s*/?\\s*");
	
	public ServerConfig(){}
	
	public ServerConfig(Flow flow){
		this.flow=flow;
		this.pluginPath=flow.transferPath;
	}
	
	/**
	 * @param config
	 */
	public ServerConfig config() {
		//配置文件
		File configFile=new File(pluginPath,"transfer.properties");
		config=PropertiesReader.getProperties(configFile);
		
		//配置参数
		initHostAddress();
		
		String httpPullIntervalMillStrs=config.getProperty("httpPullIntervalMills","").trim();
		this.httpPullIntervalMills=httpPullIntervalMillStrs.isEmpty()?5000L:Long.parseLong(httpPullIntervalMillStrs);
		
		//转存文件
		String transferSaveFileName=config.getProperty("transferSaveFile","").trim();
		if(transferSaveFileName.isEmpty()) {
			transferSaveFile=new File(flow.sharePath,"buffer.log.0");
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
		
		return this;
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress(){
		String connStrs=config.getProperty("connStr","").trim();
		Matcher matcher=CONNSTR_REGEX.matcher(connStrs.isEmpty()?"tcp://127.0.0.1:1567":connStrs.toLowerCase());
		if(!matcher.find() || 3!=matcher.groupCount()) {
			this.host="127.0.0.1";
			this.protocol="tcp";
			this.port=1567;
		}else{
			this.host=matcher.group(2).trim();
			this.protocol=matcher.group(1).trim();
			this.port=Integer.parseInt(matcher.group(3).trim());
		}
		this.connStr=new StringBuilder(protocol).append("://").append(host).append(":").append(port).toString();
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
	 * 刷新文件检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		config.setProperty("transferSaveFile", transferSaveFile.getAbsolutePath());
		OutputStream fos=null;
		try{
			fos=new FileOutputStream(new File(pluginPath,"transfer.properties"));
			log.info("reflesh checkpoint...");
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
			Field field=ServerConfig.class.getDeclaredField(attrName);
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
			Field field=ServerConfig.class.getDeclaredField(attrName);
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
		map.put("host", host);
		map.put("connStr", connStr);
		map.put("protocol", protocol);
		map.put("pluginPath", pluginPath);
		map.put("transferSaveFile", transferSaveFile);
		map.put("transferSaveMaxSize", transferSaveMaxSize);
		return map.toString();
	}
}
