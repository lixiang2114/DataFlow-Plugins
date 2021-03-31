package com.df.plugin.source.ma.file.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
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
	 * 离线推送的文件
	 */
	public File manualFile;
	
	/**
	 * 离线推送的目录
	 */
	public File manualPath;
	
	/**
	 * 离线文件已经读取的行数
	 */
	public int lineNumber;
	
	/**
	 * 离线文件已经读取的字节数量
	 */
	public long byteNumber;
	
	/**
	 * 已读取文件名检查点
	 */
	public Path readedSetFile;
	
	/**
	 * 扫描类型
	 */
	public ScanType scanType;
	
	/**
	 * 是否启用多线程处理文件
	 */
	public boolean multiThread;
	
	/**
	 * 日志(检查点)配置
	 */
	public Properties loggerConfig;
	
	/**
	 * 已读取的文件列表
	 */
	public HashSet<String> readedFiles;
	
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
		this.pluginPath=(this.flow=flow).sourcePath;
	}
	
	/**
	 * @param config
	 */
	public FileConfig config() {
		File configFile=new File(pluginPath,"source.properties");
		loggerConfig=PropertiesReader.getProperties(configFile);
		
		String scanTypeStr=loggerConfig.getProperty("scanType","").trim();
		this.scanType=scanTypeStr.isEmpty()?ScanType.file:ScanType.valueOf(scanTypeStr);
		
		if(ScanType.file==scanType) {
			String manualFileName=loggerConfig.getProperty("manualFile","").trim();
			if(manualFileName.isEmpty()) {
				log.error("not found parameter: 'manualFile',can not offline send with manual...");
				throw new RuntimeException("not found parameter: 'manualFile',can not offline send with manual...");
			}else{
				File file=new File(manualFileName);
				if(!file.exists()){
					log.error("specify file is not exists: {}",manualFileName);
					throw new RuntimeException("specify file is not exists: "+manualFileName);
				}
				
				if(file.isDirectory()){
					log.error("specify file is an directory: {}",manualFileName);
					throw new RuntimeException("specify file is an directory: "+manualFileName);
				}
				
				manualFile=file;
			}
		}else{
			String manualPathName=loggerConfig.getProperty("manualPath","").trim();
			if(manualPathName.isEmpty()) {
				log.error("not found parameter: 'manualPath',can not offline send with manual...");
				throw new RuntimeException("not found parameter: 'manualPath',can not offline send with manual...");
			}else{
				File file=new File(manualPathName);
				if(!file.exists()){
					log.error("specify path is not exists: {}",manualPathName);
					throw new RuntimeException("specify path is not exists: "+manualPathName);
				}
				
				if(file.isFile()){
					log.error("specify path is an file: {}",manualPathName);
					throw new RuntimeException("specify path is an file: "+manualPathName);
				}
				
				manualPath=file;
			}
		}
		
		String multiThreadStr=loggerConfig.getProperty("multiThread","").trim();
		this.multiThread=multiThreadStr.isEmpty()?false:Boolean.parseBoolean(multiThreadStr);
		
		if(!multiThread && ScanType.path==scanType) {
			String readedFileStr=loggerConfig.getProperty("readedFile","").trim();
			this.readedSetFile=new File(pluginPath,readedFileStr.isEmpty()?"readedFiles.ini":readedFileStr).toPath();
			try {
				this.readedFiles=new HashSet<String>(Files.readAllLines(readedSetFile, StandardCharsets.UTF_8));
			} catch (IOException e) {
				log.error("read file readed occur error...",e);
			}
		}
		
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
		if(null!=manualFile) loggerConfig.setProperty("manualFile", manualFile.getAbsolutePath());
		if(null!=manualPath) loggerConfig.setProperty("manualPath", manualPath.getAbsolutePath());
		
		if(null!=readedFiles && !readedFiles.isEmpty()) {
			if(null!=readedSetFile) Files.write(readedSetFile, readedFiles, StandardCharsets.UTF_8, StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING);
		}
		
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
		map.put("scanType", scanType);
		map.put("pluginPath", pluginPath);
		map.put("manualFile", manualFile);
		map.put("readedFiles", readedFiles);
		map.put("manualPath", manualPath);
		map.put("multiThread", multiThread);
		map.put("lineNumber", lineNumber);
		map.put("byteNumber", byteNumber);
		map.put("readedSetFile", readedSetFile);
		return map.toString();
	}
}
