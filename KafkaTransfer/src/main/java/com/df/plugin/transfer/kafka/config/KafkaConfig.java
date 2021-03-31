package com.df.plugin.transfer.kafka.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.SizeUnit;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description KafKa客户端配置
 */
@SuppressWarnings("unchecked")
public class KafkaConfig {
	/**
	 * 流程实例
	 */
	public Flow flow;
	
	/**
	 * 连接主题名称
	 */
	public String topic;
	
	/**
	 * 插件运行时路径
	 */
	public File pluginPath;
	
	/**
	 * 主机列表
	 */
	private String hostList;
	
	/**
	 * 转存目录
	 */
	public File transferPath;
	
	/**
	 * KafKa客户端配置
	 */
	private Properties config;
	
	/**
	 * 实时转存的日志文件
	 */
	public File transferSaveFile;
	
	/**
	 * 管理客户端
	 */
	public AdminClient adminClient;
	
	/**
	 * 转存日志文件最大尺寸
	 */
	public Long transferSaveMaxSize;
	
	/**
	 * 是否可以自动创建主题
	 */
	public boolean autoCreateTopic;
	
	/**
	 * 每个主题的分区数量
	 */
	public int partitionNumPerTopic;
	
	/**
	 * 消费者组ID
	 */
	private String consumerGroupId;
	
	/**
	 * 会话超时(单位:毫秒)
	 */
	private String sessionTimeoutMills;
	
	/**
	 * 批量拉取超时时间(单位:毫秒)
	 */
	public Duration batchPollTimeoutMills;
	
	/**
	 * 自动提交间隔时间(单位:毫秒)
	 */
	private String autoCommitIntervalMills;
	
	/**
	 * KafKa消费者客户端
	 */
	public KafkaConsumer<String, String> consumer;
	
	/**
	 * Kafka默认客户端端口
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
	public static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(KafkaConfig.class);
	
	/**
     * IP地址正则式
     */
	public static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	/**
	 * 键反序列化器
	 */
	private static final String KEY_DESERIALIZER="org.apache.kafka.common.serialization.StringDeserializer";
	
	/**
	 * 值反序列化器
	 */
	private static final String VALUE_DESERIALIZER="org.apache.kafka.common.serialization.StringDeserializer";
	
	public KafkaConfig(){}
	
	public KafkaConfig(Flow flow){
		this.flow=flow;
		this.transferPath=flow.sharePath;
		this.pluginPath=flow.transferPath;
		this.config=PropertiesReader.getProperties(new File(pluginPath,"transfer.properties"));
	}
	
	/**
	 * @param config
	 */
	public KafkaConfig config() {
		String transferSaveFileName=config.getProperty("transferSaveFile","").trim();
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
		
		log.info("transfer save logger file is: "+transferSaveFile.getAbsolutePath());
		
		transferSaveMaxSize=getTransferSaveMaxSize();
		log.info("transfer save logger file max size is: "+transferSaveMaxSize);
		
		String topicStr=config.getProperty("topic","").trim();
		this.topic=topicStr.isEmpty()?null:topicStr;
		if(null==topic) {
			log.error("topic can not be empty...");
			throw new RuntimeException("topic can not be empty...");
		}
		
		String hostStr=config.getProperty("hostList","").trim();
		String autoCreateTopicStr=config.getProperty("autoCreateTopic","").trim();
		this.autoCreateTopic=autoCreateTopicStr.isEmpty()?true:Boolean.parseBoolean(autoCreateTopicStr);
		
		String consumerGroupIdStr=config.getProperty("consumerGroupId","").trim();
		this.consumerGroupId=consumerGroupIdStr.isEmpty()?"lixiang2114":consumerGroupIdStr;
		
		String sessionTimeoutMillsStr=config.getProperty("sessionTimeoutMills","").trim();
		this.sessionTimeoutMills=sessionTimeoutMillsStr.isEmpty()?"30000":sessionTimeoutMillsStr;
		
		String batchPollTimeoutMillStr=config.getProperty("batchPollTimeoutMills","").trim();
		this.batchPollTimeoutMills=Duration.ofMillis(batchPollTimeoutMillStr.isEmpty()?100:Long.parseLong(batchPollTimeoutMillStr));
		
		String partitionNumPerTopicStr=config.getProperty("partitionNumPerTopic","").trim();
		this.partitionNumPerTopic=partitionNumPerTopicStr.isEmpty()?1:Integer.parseInt(partitionNumPerTopicStr);
		
		String autoCommitIntervalMillsStr=config.getProperty("autoCommitIntervalMills","").trim();
		this.autoCommitIntervalMills=autoCommitIntervalMillsStr.isEmpty()?"1000":autoCommitIntervalMillsStr;
		
		configHostAddress(hostStr);
		configKafKaClient();
		
		return this;
	}
	
	/**
	 * 初始化Kafka服务客户端
	 * @param hosts 主机地址
	 */
	private void configKafKaClient() {
		Properties props = new Properties();
		props.put("auto.commit.interval.ms", autoCommitIntervalMills);
    	props.setProperty("value.deserializer",VALUE_DESERIALIZER);
    	props.setProperty("session.timeout.ms",sessionTimeoutMills);
    	props.setProperty("key.deserializer",KEY_DESERIALIZER);
    	props.setProperty("group.id",consumerGroupId);
    	props.setProperty("bootstrap.servers",hostList);
    	props.put("enable.auto.commit", "true");
    	adminClient=AdminClient.create(props);
    	consumer=new KafkaConsumer<String, String>(props);
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
		map.put("topic", topic);
		map.put("hostList", hostList);
		map.put("transferPath", transferPath);
		map.put("transferSaveFile", transferSaveFile);
		map.put("autoCreateTopic", autoCreateTopic);
		map.put("consumerGroupId", consumerGroupId);
		map.put("sessionTimeoutMills", sessionTimeoutMills);
		map.put("transferSaveMaxSize", transferSaveMaxSize);
		map.put("partitionNumPerTopic", partitionNumPerTopic);
		map.put("autoCommitIntervalMills", autoCommitIntervalMills);
		map.put("batchPollTimeoutMills", batchPollTimeoutMills.toMillis());
		return map.toString();
	}
}
