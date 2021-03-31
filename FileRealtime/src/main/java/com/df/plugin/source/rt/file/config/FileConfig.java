package com.df.plugin.source.rt.file.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
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
	 * 插件目录
	 */
	public File pluginPath;
	
	/**
	 * 转存目录
	 */
	public File transferPath;
	
	/**
	 * 缓冲日志文件
	 */
	public File loggerFile;
	
	/**
	 * 实时文件已经读取的行数
	 */
	public int lineNumber;
	
	/**
	 * 实时文件已经读取的字节数量
	 */
	public long byteNumber;
	
	/**
	 * 数据扫描模式
	 */
	public ScanMode scanMode;
	
	/**
	 * 缓冲日志文件被读完后自动删除
	 */
	public Boolean delOnReaded;
	
	/**
	 * 日志(检查点)配置
	 */
	public Properties loggerConfig;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileConfig.class);
	
	public FileConfig(){}
	
	public FileConfig(Flow flow){
		this.flow=flow;
		transferPath=flow.sharePath;
		this.pluginPath=flow.sourcePath;
	}
	
	/**
	 * @param config
	 */
	public FileConfig config() {
		//读取日志文件配置
		File configFile=new File(pluginPath,"source.properties");
		loggerConfig=PropertiesReader.getProperties(configFile);
		
		//数据扫描模式
		String scanModeStr=loggerConfig.getProperty("scanMode","").trim();
		scanMode=scanModeStr.isEmpty()?ScanMode.file:ScanMode.valueOf(scanModeStr);
		
		if(ScanMode.channel==scanMode) return this;
		
		//实时自动推送的日志文件
		String loggerFileName=loggerConfig.getProperty("loggerFile","").trim();
		if(loggerFileName.isEmpty()) {
			loggerFile=new File(transferPath,"buffer.log.0");
			log.warn("not found parameter: 'loggerFile',will be use default...");
		}else{
			File file=new File(loggerFileName);
			if(file.exists() && file.isDirectory()) {
				log.error("loggerFile can not be directory...");
				throw new RuntimeException("loggerFile can not be directory...");
			}
			
			File filePath=file.getParentFile();
			if(!filePath.exists()) filePath.mkdirs();
			
			if(!file.exists()) {
				log.warn("file: {} is not exists,it will be create...",file.getAbsolutePath());
				try {
					file.createNewFile();
				} catch (IOException e) {
					log.error("create logger file occur error:",e);
					throw new RuntimeException(e);
				}
			}
			
			String fileFullName=file.getAbsolutePath();
			if(!fileFullName.contains(".")) file.renameTo(new File(fileFullName+".0"));
			loggerFile=file;
		}
		
		log.info("realTime logger file is: "+loggerFile.getAbsolutePath());
		
		//是否自动删除缓冲日志文件
		String delOnReadedStr=loggerConfig.getProperty("delOnReaded","").trim();
		this.delOnReaded=delOnReadedStr.isEmpty()?true:Boolean.parseBoolean(delOnReadedStr);
		
		//实时自动推送的日志文件检查点
		String lineNumberStr=loggerConfig.getProperty("lineNumber","").trim();
		this.lineNumber=lineNumberStr.isEmpty()?0:Integer.parseInt(lineNumberStr);
		
		String byteNumberStr=loggerConfig.getProperty("byteNumber","").trim();
		this.byteNumber=byteNumberStr.isEmpty()?0:Integer.parseInt(byteNumberStr);
		
		log.info("lineNumber is: "+lineNumber+",byteNumber is: "+byteNumber);
		return this;
	}
	
	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		loggerConfig.setProperty("lineNumber",""+lineNumber);
		loggerConfig.setProperty("byteNumber",""+byteNumber);
		loggerConfig.setProperty("loggerFile", loggerFile.getAbsolutePath());
		
		OutputStream fos=null;
		try{
			fos=new FileOutputStream(new File(pluginPath,"source.properties"));
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
	 * 收集实时参数
	 * @return
	 */
	public String collectRealtimeParams() {
		HashMap<String,Object> map=new HashMap<String,Object>();
		map.put("loggerFile", loggerFile);
		map.put("scanMode", scanMode);
		map.put("pluginPath", pluginPath);
		map.put("lineNumber", lineNumber);
		map.put("byteNumber", byteNumber);
		map.put("delOnReaded", delOnReaded);
		return map.toString();
	}
}
