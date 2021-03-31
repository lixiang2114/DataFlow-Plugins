package com.df.plugin.transfer.file.config;

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
 * @description 文件配置
 */
@SuppressWarnings("unchecked")
public class FileConfig {
	/**
	 * 流程对象
	 */
	public Flow flow;
	
	/**
	 * tail进程ID文件路径
	 */
	public File tailPidPath;
	
	/**
	 * 插件运行时路径
	 */
	public File pluginPath;
	
	/**
	 * 转存目录
	 */
	public File transferPath;
	
	/**
	 * 应用端日志文件
	 */
	public String appLogFile;
	
	/**
	 * 实时转存的日志文件
	 */
	public File transferSaveFile;
	
	/**
	 * 是否为Windows系统
	 */
	public static Boolean isWin;
	
	/**
	 * 日志(检查点)配置
	 */
	public Properties loggerConfig;
	
	/**
	 * 转存日志文件最大尺寸
	 */
	public Long transferSaveMaxSize;
	
	/**
	 * WinNT下的Tailf命令字串
	 */
	public static String winTailfCmd;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileConfig.class);
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	public FileConfig(){}
	
	public FileConfig(Flow flow){
		this.flow=flow;
		this.transferPath=flow.sharePath;
		this.pluginPath=flow.transferPath;
		this.tailPidPath=new File(pluginPath,"tail/logs/pid");
	}
	
	/**
	 * @param config
	 */
	public FileConfig config() {
		//配置基础参数
		isWin=CommonUtil.getOSType().toLowerCase().startsWith("win");
		configWinTailfCmd();
		
		//读取日志文件配置
		File configFile=new File(pluginPath,"transfer.properties");
		loggerConfig=PropertiesReader.getProperties(configFile);
		
		//应用方日志文件
		String appLogFileStr=loggerConfig.getProperty("appLogFile","").trim();
		if(appLogFileStr.isEmpty()) {
			log.error("appLogFile can not be NULL...");
			throw new RuntimeException("appLogFile can not be NULL...");
		}else{
			this.appLogFile=appLogFileStr;
		}
		
		//转存日志文件
		String transferSaveFileName=loggerConfig.getProperty("transferSaveFile","").trim();
		if(transferSaveFileName.isEmpty()) {
			transferSaveFile=new File(transferPath,"buffer.log.0");
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
	 * 获取转存日志文件最大尺寸(默认为2GB)
	 */
	private Long getTransferSaveMaxSize(){
		String configMaxVal=loggerConfig.getProperty("transferSaveMaxSize","").trim();
		if(configMaxVal.isEmpty()) return 2*1024*1024*1024L;
		Matcher matcher=CAP_REGEX.matcher(configMaxVal);
		if(!matcher.find()) return 2*1024*1024*1024L;
		return SizeUnit.getBytes(Long.parseLong(matcher.group(1)), matcher.group(2).substring(0,1));
	}
	
	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		loggerConfig.setProperty("transferSaveFile", transferSaveFile.getAbsolutePath());
		OutputStream fos=null;
		try{
			fos=new FileOutputStream(new File(pluginPath,"transfer.properties"));
			log.info("reflesh checkpoint...");
			loggerConfig.store(fos, "reflesh checkpoint");
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
			Field field=FileConfig.class.getDeclaredField(attrName);
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
			Field field=FileConfig.class.getDeclaredField(attrName);
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
	 * 查找WindowNT下的tailf命令
	 */
	public void configWinTailfCmd() {
		if(!isWin) return;
		File tailfCmd=new File(pluginPath,"tail/bin/tailf.exe");
		if(tailfCmd.exists()) {
			winTailfCmd=tailfCmd.getAbsolutePath();
		}else{
			File systemTailfCmd=new File(System.getenv("SystemDrive")+"/windows/system32/tailf.exe");
			if(systemTailfCmd.exists()){
				winTailfCmd=systemTailfCmd.getAbsolutePath();
			}else{
				throw new RuntimeException("current system is windows,tailf.exe is not found...");
			}
		}
		log.info("current system is windows,tailf path is: {}",winTailfCmd);
	}
	
	/**
	 * 收集实时参数
	 * @return
	 */
	public String collectRealtimeParams() {
		HashMap<String,Object> map=new HashMap<String,Object>();
		map.put("isWin", isWin);
		map.put("pluginPath", pluginPath);
		map.put("appLogFile", appLogFile);
		map.put("winTailfCmd", winTailfCmd);
		map.put("transferSaveFile", transferSaveFile);
		map.put("transferSaveMaxSize", transferSaveMaxSize);
		return map.toString();
	}
}
