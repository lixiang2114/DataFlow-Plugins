package com.df.plugin.source.ma.hbase.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
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
 * @description Hbase分布式存储客户端配置
 */
@SuppressWarnings("unchecked")
public class HbaseConfig {
	/**
	 * 流程对象
	 */
	public Flow flow;
	
	/**
	 * 页面记录数(批处理尺寸)
	 */
	public int batchSize;
	
	/**
	 * 插件运行时路径
	 */
	public File pluginPath;
	
	/**
	 * 扫描的目标库表
	 */
	public String targetTab;
	
	/**
	 * 输出数据格式
	 * qstr: 查询字串格式 
	 * map: 字典Json格式
	 */
	public String outFormat;
	
	/**
	 * Hbase插件配置
	 */
	public Properties config;
	
	/**
	 * 分页起始行键
	 */
	public String startRowKey;
	
	/**
	 * 字段分隔符
	 */
	public String fieldSeparator;
	
	/**
	 * Zookeper主机列表
	 */
	public ArrayList<String> hostList=new ArrayList<String>();
	
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
     * 数字正则式
     */
	private static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HbaseConfig.class);
	
	/**
     * IP地址正则式
     */
	private static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	public HbaseConfig(){}
	
	public HbaseConfig(Flow flow){
		this.pluginPath=(this.flow=flow).sourcePath;
		this.config=PropertiesReader.getProperties(new File(pluginPath,"source.properties"));
	}
	
	/**
	 * @param config
	 */
	public HbaseConfig config() throws IOException{
		initHostAddress();
		if(hostList.isEmpty()) {
			log.error("hostList parameter must be specify...");
			throw new RuntimeException("hostList parameter must be specify...");
		}
		
		this.targetTab=config.getProperty("targetTab","").trim();
		if(targetTab.isEmpty()) {
			log.error("targetTab parameter can not be empty...");
			throw new RuntimeException("targetTab parameter can not be empty...");
		}
		
		String outFormatStr=config.getProperty("outFormat","").trim();
		this.outFormat=outFormatStr.isEmpty()?"qstr":outFormatStr;
		
		String fieldSeparatorStr=config.getProperty("fieldSeparator", "").trim();
		this.fieldSeparator=fieldSeparatorStr.isEmpty()?"#":fieldSeparatorStr;
		
		String batchSizeStr=config.getProperty("batchSize", "").trim();
		this.batchSize=batchSizeStr.isEmpty()?100:Integer.parseInt(batchSizeStr);
		
		this.startRowKey=config.getProperty("startRowKey","").trim();
		log.info("startRowKey is: "+startRowKey+",batchSize is: "+batchSize);
		
		return this;
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress() {
		String hostListStr=config.getProperty("hostList","").trim();
		String[] hosts=hostListStr.isEmpty()?new String[]{"127.0.0.1:2181"}:COMMA_REGEX.split(hostListStr);
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(host.isEmpty()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2) {
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				hostList.add(ip+":"+port);
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				hostList.add("127.0.0.1:"+unknow);
			}else if(IP_REGEX.matcher(unknow).matches()){
				hostList.add(unknow+":2181");
			}
		}
	}
	
	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		config.setProperty("startRowKey",startRowKey);
		OutputStream fos=null;
		try{
			fos=new FileOutputStream(new File(pluginPath,"source.properties"));
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
			Field field=HbaseConfig.class.getDeclaredField(attrName);
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
			Field field=HbaseConfig.class.getDeclaredField(attrName);
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
		map.put("hostList", hostList);
		map.put("targetTab", targetTab);
		map.put("batchSize", batchSize);
		map.put("outFormat", outFormat);
		map.put("pluginPath", pluginPath);
		map.put("startRowKey", startRowKey);
		map.put("fieldSeparator", fieldSeparator);
		return map.toString();
	}
}
