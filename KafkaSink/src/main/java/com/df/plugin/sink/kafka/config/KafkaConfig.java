package com.df.plugin.sink.kafka.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description Kafka客户端配置
 * KafKa客户端自身具备批处理能力和重试功能,故无需单独设计批处理和重试逻辑
 */
@SuppressWarnings("unchecked")
public class KafkaConfig {
	/**
	 * 发送器运行时路径
	 */
	private File sinkPath;
	
	/**
	 * 主机列表
	 */
	private String hostList;
	
	/**
	 * 是否解析通道记录
	 */
	private boolean parse;
	
	/**
	 * Ack确认级别
	 * acks=0:生产者不需要来自broker的确认(不安全)
	 * acks=1:broker端将消息保存后即发送ack确认(安全)
	 * acks=-1:broker端需要等待所有副本接收后才发送ack确认(最安全)
	 */
	private String ackLevel;
	
	/**
	 * 主题字段索引
	 * 默认值为0
	 */
	private int topicIndex=0;
	
	/**
	 * 主题字段名称
	 */
	private String topicField;
	
	/**
	 * Kafka客户端配置
	 */
	private Properties config;
	
	/**
	 * 默认连接主题名称
	 */
	private String defaultTopic;
	
	/**
	 * 字段分隔符
	 * 默认值为英文逗号
	 */
	private String fieldSeparator;
	
	/**
	 * 发送失败后最大重试次数
	 */
	private String maxRetryTimes;
	
	/**
	 * 管理客户端
	 */
	public AdminClient adminClient;
	
	/**
	 * 是否可以自动创建主题
	 */
	public boolean autoCreateTopic;
	
	/**
	 * 每个主题的分区数量
	 */
	public int partitionNumPerTopic;
	
	/**
	 * 上次发送失败任务表
	 */
	public Set<Object> preFailSinkSet;
	
	/**
	 * KafKa生产者客户端
	 */
	public KafkaProducer<String, String> producer;
	
	/**
	 * 默认TCP协议端口
	 */
	private static final String DEFAULT_PORT="9092";
	
	/**
	 * 默认主机地址
	 */
	private static final String DEFAULT_HOST="127.0.0.1";
	
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
     * IP地址正则式
     */
	private static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	/**
	 * 键序列化器
	 */
	private static final String KEY_SERIALIZER="org.apache.kafka.common.serialization.StringSerializer";
	
	/**
	 * 值序列化器
	 */
	private static final String VALUE_SERIALIZER="org.apache.kafka.common.serialization.StringSerializer";
	
	public KafkaConfig(){}
	
	public KafkaConfig(Flow flow){
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	public boolean getParse() {
		return parse;
	}
	
	public String getTopicField(){
		return topicField;
	}
	
	public int getTopicIndex(){
		return topicIndex;
	}
	
	public String getHostList() {
		return hostList;
	}
	
	public String getFieldSeparator() {
		return fieldSeparator;
	}
	
	public String getDefaultTopic() {
		return defaultTopic;
	}
	
	/**
	 * @param config
	 */
	public KafkaConfig config() {
		String parseStr=config.getProperty("parse","").trim();
		String hostStr=config.getProperty("hostList","").trim();
		String ackLevelStr=config.getProperty("ackLevel","").trim();
		String topicFieldStr=config.getProperty("topicField","").trim();
		String topicIndexStr=config.getProperty("topicIndex","").trim();
		String defaultTopicStr=config.getProperty("defaultTopic","").trim();
		String fieldSeparatorStr=config.getProperty("fieldSeparator","").trim();
		String maxRetryTimesStr=config.getProperty("maxRetryTimes","").trim();
		String autoCreateTopicStr=config.getProperty("autoCreateTopic","").trim();
		String partitionNumPerTopicStr=config.getProperty("partitionNumPerTopic","").trim();
		
		this.ackLevel=ackLevelStr.isEmpty()?"1":ackLevelStr;
		this.topicField=topicFieldStr.isEmpty()?"topic":topicFieldStr;
		this.defaultTopic=defaultTopicStr.isEmpty()?null:defaultTopicStr;
		this.parse=parseStr.isEmpty()?true:Boolean.parseBoolean(parseStr);
		this.fieldSeparator=fieldSeparatorStr.isEmpty()?",":fieldSeparatorStr;
		this.maxRetryTimes=maxRetryTimesStr.isEmpty()?"3":maxRetryTimesStr;
		this.topicIndex=topicIndexStr.isEmpty()?0:Integer.parseInt(topicIndexStr);
		this.autoCreateTopic=autoCreateTopicStr.isEmpty()?true:Boolean.parseBoolean(autoCreateTopicStr);
		this.partitionNumPerTopic=partitionNumPerTopicStr.isEmpty()?1:Integer.parseInt(partitionNumPerTopicStr);
		
		configHostAddress(hostStr);
		configKafKaClient(maxRetryTimes,ackLevel);
		
		return this;
	}
	
	/**
	 * 初始化Kafka服务客户端
	 * @param maxRetries 最大重试次数
	 * @param ackLevel ACK确认级别
	 */
	private void configKafKaClient(String maxRetries,String ackLevel) {
		Properties props = new Properties();
    	props.setProperty("value.serializer",VALUE_SERIALIZER);
    	props.setProperty("key.serializer",KEY_SERIALIZER);
    	props.setProperty("max.request.size","10485760");
    	props.setProperty("buffer.memory","536870912");
    	props.setProperty("bootstrap.servers",hostList);
    	props.setProperty("batch.size","163840");
    	props.setProperty("retries",maxRetries);
    	props.setProperty("acks",ackLevel);
    	adminClient=AdminClient.create(props);
    	producer=new KafkaProducer<String, String>(props);
	}
	
	/**
	 * 初始化Kafka主机地址
	 * @param hosts 主机地址
	 */
	private void configHostAddress(String hosts) {
		String hostStr=hosts.isEmpty()?(DEFAULT_HOST+":"+DEFAULT_PORT):hosts;
		ArrayList<String> tmpList=new ArrayList<String>();
		String[] hostArray=COMMA_REGEX.split(hostStr);
		for(int i=0;i<hostArray.length;i++){
			String host=hostArray[i].trim();
			if(host.isEmpty()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length<=0) continue;
			
			if(ipAndPort.length>=2){
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				tmpList.add(new StringBuilder(ip).append(":").append(port).toString());
				continue;
			}
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				tmpList.add(new StringBuilder(DEFAULT_HOST).append(":").append(unknow).toString());
			}else if(IP_REGEX.matcher(unknow).matches()){
				tmpList.add(new StringBuilder(unknow).append(":").append(DEFAULT_PORT).toString());
			}
		}
		
		StringBuilder builder=new StringBuilder("");
		for(int i=0;i<tmpList.size();builder.append(",").append(tmpList.get(i++)));
		if(0!=builder.length()) hostList=builder.deleteCharAt(0).toString();
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
			Field field=KafkaConfig.class.getDeclaredField(attrName);
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
			Field field=KafkaConfig.class.getDeclaredField(attrName);
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
		map.put("parse", parse);
		map.put("hostList", hostList);
		map.put("ackLevel", ackLevel);
		map.put("topicField", topicField);
		map.put("topicIndex", topicIndex);
		map.put("defaultTopic", defaultTopic);
		map.put("fieldSeparator", fieldSeparator);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("autoCreateTopic", autoCreateTopic);
		map.put("preFailSinkSetSize", preFailSinkSet.size());
		map.put("partitionNumPerTopic", partitionNumPerTopic);
		return map.toString();
	}
}
