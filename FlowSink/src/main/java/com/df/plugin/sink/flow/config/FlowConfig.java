package com.df.plugin.sink.flow.config;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.flow.consts.RuleType;
import com.df.plugin.sink.flow.consts.SendMode;
import com.df.plugin.sink.flow.consts.TargetType;
import com.df.plugin.sink.flow.dto.FlowMapper;
import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.Context;
import com.github.lixiang2114.flow.context.SizeUnit;
import com.github.lixiang2114.flow.util.ClassLoaderUtil;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description 流程发送器配置
 */
@SuppressWarnings("unchecked")
public class FlowConfig {
	/**
	 * 流程实例对象
	 */
	public Flow flow;
	
	/**
	 * 发送器运行时路径
	 */
	private File sinkPath;
	
	/**
	 * 入口类名
	 */
	public String mainClass;
	
	/**
	 * 入口函数名
	 */
	public String mainMethod;
	
	/**
	 * 按字段分发时的项键
	 * index类型默认值为0
	 * key类型默认值为flows
	 */
	public String itemKey;
	
	/**
	 * 按字段分发时的规则类型
	 * 默认为index类型
	 */
	public RuleType ruleType;
	
	/**
	 * 按字段分发时项分隔符正则式
	 * 默认为英文逗号
	 */
	public Pattern itemRegex;
	
	/**
	 * 按字段分发时字段分隔符正则式
	 * 默认值为英文叹号
	 */
	public Pattern fieldRegex;
	
	/**
	 * 目标存储项目列表
	 */
	public String[] targetItems;
	
	/**
	 * 转发目标模式
	 */
	public SendMode sendMode;
	
	/**
	 * 文件客户端配置
	 */
	private Properties loggerConfig;
	
	/**
	 * 转存目标文件最大尺寸
	 */
	public Long targetFileMaxSize;
	
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern EQUAL_REGEX=Pattern.compile("=");
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FlowConfig.class);
	
	/**
	 * 实时转存流程表
	 */
	public ArrayList<FlowMapper> targetFlowList=new ArrayList<FlowMapper>();
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	public FlowConfig(){}
	
	public FlowConfig(Flow flow){
		this.sinkPath=(this.flow=flow).sinkPath;
		this.loggerConfig=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * 配置文件发送器
	 * @param config
	 * @throws IOException 
	 */
	public FlowConfig config() throws Exception {
		//转发项列表
		String dispatcherItemStr=loggerConfig.getProperty("dispatcherItems", "").trim();
		if(dispatcherItemStr.isEmpty()) {
			log.error("dispatcherItems is empty...");
			throw new RuntimeException("dispatcherItems is empty...");
		}
		
		//转存日志文件最大尺寸
		this.targetFileMaxSize=getTargetFileMaxSize();
		log.info("target file max size is: "+targetFileMaxSize);
		
		//转发模式
		String sendModeStr=loggerConfig.getProperty("sendMode","").trim();
		sendMode=sendModeStr.isEmpty()?SendMode.rep:SendMode.valueOf(sendModeStr);
		
		//目标项目列表
		String[] typeAndItemStrArray=COMMA_REGEX.split(dispatcherItemStr);
		this.targetItems=new String[typeAndItemStrArray.length];
		
		//转存文件列表
		LinkedHashSet<FlowMapper> transferMapperSet=new LinkedHashSet<FlowMapper>();
		for(int i=0;i<typeAndItemStrArray.length;i++) {
			String typeAndItemStr=typeAndItemStrArray[i].trim();
			if(typeAndItemStr.isEmpty()) continue;
			
			String[] typeAndItem=EQUAL_REGEX.split(typeAndItemStr);
			if(0==typeAndItem.length) continue;
			
			String first=typeAndItem[0].trim();
			String second=1<typeAndItem.length?typeAndItem[1].trim():"";
			if(first.isEmpty() && second.isEmpty()) continue;
			
			String type=null;
			String item=null;
			if(first.isEmpty() || second.isEmpty()) {
				type="buffer";
				item=first.isEmpty()?second:first;
			}else{
				type=typeAndItem[0].trim();
				item=typeAndItem[1].trim();
			}
			
			this.targetItems[i]=item;
			TargetType targetType=TargetType.valueOf(type);
			if(TargetType.file==targetType) {
				File file=new File(item);
				if(file.exists() && file.isDirectory()) {
					log.error("target file: {} can not be directory...",file.getAbsolutePath());
					throw new RuntimeException("target file: "+file.getAbsolutePath()+" can not be directory...");
				}
				transferMapperSet.add(new FlowMapper(file,this));
			}else{
				Flow targetFlow=Context.getFlow(item);
				if(null==targetFlow) {
					log.error("flow: {} is not exists",item);
					throw new RuntimeException("flow: "+item+" is not exists");
				}
				transferMapperSet.add(new FlowMapper(targetFlow,targetType,this));
			}
		}
		
		this.targetFlowList.addAll(transferMapperSet);
		log.info("target items are: "+transferMapperSet);
		
		if(SendMode.field==sendMode) {
			String ruleStr=loggerConfig.getProperty("rule","").trim();
			String itemSeparator=loggerConfig.getProperty("itemSeparator","").trim();
			this.ruleType=ruleStr.isEmpty()?RuleType.index:RuleType.valueOf(ruleStr);
			this.itemRegex=Pattern.compile(itemSeparator.isEmpty()?"#":itemSeparator);
			if(RuleType.key==ruleType) { //key类型
				String keyStr=loggerConfig.getProperty(RuleType.key.typeName,"").trim();
				this.itemKey=keyStr.isEmpty()?"flows":keyStr;
			}else{ //index类型
				String fieldSeparator=loggerConfig.getProperty("fieldSeparator","").trim();
				String keyStr=loggerConfig.getProperty(RuleType.index.typeName,"").trim();
				this.itemKey=keyStr.isEmpty()?"0":keyStr;
				this.fieldRegex=Pattern.compile(fieldSeparator.isEmpty()?",":fieldSeparator);
			}
		}
		
		if(SendMode.custom==sendMode) {
			File libPath=new File(sinkPath,"lib");
			File binPath=new File(sinkPath,"bin");
			File srcPath=new File(sinkPath,"script");
			File appPath=new File(Context.projectFile,"lib");
			
			if(ClassLoaderUtil.compileJavaSource(srcPath,binPath,new File[]{appPath,libPath})) {
				ClassLoaderUtil.addFileToCurrentClassPath(binPath);
			}else{
				String srcPathName=srcPath.getAbsolutePath();
				log.error("compile script failure: {}",srcPathName);
				throw new RuntimeException("compile script failure: "+srcPathName);
			}
			
			String mainClassStr=loggerConfig.getProperty("mainClass","").trim();
			this.mainClass=mainClassStr.isEmpty()?"DefaultClass":mainClassStr;
			
			String mainMethodStr=loggerConfig.getProperty("mainMethod","").trim();
			this.mainMethod=mainMethodStr.isEmpty()?"main":mainMethodStr;
		}
		
		return this;
	}
	
	/**
	 * 获取转存日志文件最大尺寸(默认为2GB)
	 */
	private Long getTargetFileMaxSize(){
		String configMaxVal=loggerConfig.getProperty("targetFileMaxSize","").trim();
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
			Field field=FlowConfig.class.getDeclaredField(attrName);
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
			Field field=FlowConfig.class.getDeclaredField(attrName);
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
		map.put("sinkPath", sinkPath);
		map.put("sendMode", sendMode);
		map.put("targetFlowList", targetFlowList);
		map.put("targetFileMaxSize", targetFileMaxSize);
		return map.toString();
	}
}
