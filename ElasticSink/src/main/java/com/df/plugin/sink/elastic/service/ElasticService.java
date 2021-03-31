package com.df.plugin.sink.elastic.service;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.elastic.config.ElasticConfig;
import com.df.plugin.sink.elastic.dto.CollectionMapper;
import com.df.plugin.sink.elastic.util.ElasticUtil;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description Elastic服务
 */
@SuppressWarnings("unchecked")
public class ElasticService {
	/**
	 * Elastic工具
	 */
	private ElasticUtil elasticUtil;
	
	/**
	 * Elastic配置
	 */
	private ElasticConfig elasticConfig;
	
	/**
	 * 空白正则式
	 */
	private static final Pattern BLANK_REGEX=Pattern.compile("\\s+");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ElasticService.class);
	
	/**
	 * 批量文档表
	 */
	public ArrayList<HashMap<String,Object>> batchList=new ArrayList<HashMap<String,Object>>();
	
	public ElasticService(){}
	
	public ElasticService(ElasticConfig elasticConfig){
		this.elasticConfig=elasticConfig;
		this.elasticUtil=new ElasticUtil(elasticConfig.restClient);
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws InterruptedException
	 */
	public boolean preSend() throws InterruptedException {
		if(0==elasticConfig.preFailSinkSet.size())  return true;
		for(Object object:elasticConfig.preFailSinkSet) {
			CollectionMapper<HashMap<String,Object>> collectionMapper=(CollectionMapper<HashMap<String,Object>>)object;
			collectionMapper.setElasticConfig(elasticConfig);
			if(!collectionMapper.send()) return false;
			elasticConfig.preFailSinkSet.remove(object);
		}
		return true;
	}
	
	/**
	 * 解析通道消息并发送到Elastic存储
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean parseAndSingleSend(String msg) throws InterruptedException {
		HashMap<String,Object> docMap=new HashMap<String,Object>();
		String[] fieldValues=elasticConfig.fieldSeparator.split(msg);
		if(null==elasticConfig.fieldList || 0==elasticConfig.fieldList.length){
			for(int i=0;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}else if(elasticConfig.fieldList.length>=fieldValues.length){
			for(int i=0;i<fieldValues.length;docMap.put(elasticConfig.fieldList[i], fieldValues[i]),i++);
		}else{
			int i=0;
			for(;i<elasticConfig.fieldList.length;docMap.put(elasticConfig.fieldList[i], fieldValues[i]),i++);
			for(;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}
		
		if(docMap.isEmpty()) return null;
		
		for(String numField:elasticConfig.numFieldSet) {
			Object value=docMap.get(numField);
			if(null==value) continue;
			docMap.put(numField, getNumber(value));
		}
		
		for(String timeField:elasticConfig.timeFieldSet) {
			Object value=docMap.get(timeField);
			if(null==value) continue;
			docMap.put(timeField, getESTimestamp(value));
		}
		
		return singleSend(docMap);
	}
	
	/**
	 * 解析通道消息并发送到Elastic存储
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean parseAndBatchSend(String msg) throws InterruptedException {
		if(null==msg) {
			if(batchList.isEmpty()) return null;
			return batchSend(batchList);
		}
		
		if((msg=msg.trim()).isEmpty()) return null;
		
		HashMap<String,Object> docMap=new HashMap<String,Object>();
		String[] fieldValues=elasticConfig.fieldSeparator.split(msg);
		if(null==elasticConfig.fieldList || 0==elasticConfig.fieldList.length){
			for(int i=0;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}else if(elasticConfig.fieldList.length>=fieldValues.length){
			for(int i=0;i<fieldValues.length;docMap.put(elasticConfig.fieldList[i], fieldValues[i]),i++);
		}else{
			int i=0;
			for(;i<elasticConfig.fieldList.length;docMap.put(elasticConfig.fieldList[i], fieldValues[i]),i++);
			for(;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}
		
		if(docMap.isEmpty()) return null;
		
		for(String numField:elasticConfig.numFieldSet) {
			Object value=docMap.get(numField);
			if(null==value) continue;
			docMap.put(numField, getNumber(value));
		}
		
		for(String timeField:elasticConfig.timeFieldSet) {
			Object value=docMap.get(timeField);
			if(null==value) continue;
			docMap.put(timeField, getESTimestamp(value));
		}
		
		batchList.add(docMap);
		if(batchList.size()<elasticConfig.batchSize) return null;
		
		return batchSend(batchList);
	}
	
	/**
	 * 不解析通道消息并实时发送到Elastic存储
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean noParseAndSingleSend(String msg) throws InterruptedException {
		HashMap<String,Object> docMap=CommonUtil.jsonStrToJava(msg, HashMap.class);
		if(null==docMap || docMap.isEmpty()) return null;
		
		for(String timeField:elasticConfig.timeFieldSet){
			Object value=docMap.get(timeField);
			if(null==value) continue;
			docMap.put(timeField, getESTimestamp(value));
		}
		
		return singleSend(docMap);
	}
	
	/**
	 * 不解析通道消息直接发送到Elastic存储
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean noParseAndBatchSend(String msg) throws InterruptedException {
		if(null==msg) {
			if(batchList.isEmpty()) return null;
			return batchSend(batchList);
		}
		
		if((msg=msg.trim()).isEmpty()) return null;
		HashMap<String,Object> docMap=CommonUtil.jsonStrToJava(msg, HashMap.class);
		if(null==docMap || docMap.isEmpty()) return null;
		
		for(String timeField:elasticConfig.timeFieldSet){
			Object value=docMap.get(timeField);
			if(null==value) continue;
			docMap.put(timeField, getESTimestamp(value));
		}
		
		batchList.add(docMap);
		if(batchList.size()<elasticConfig.batchSize) return null;
		
		return batchSend(batchList);
	}
	
	/**
	 * 发送单个文档到Elastic存储
	 * @param doc 文档对象
	 * @return 是否发送成功
	 * @throws InterruptedException 
	 */
	private boolean singleSend(HashMap<String,Object> docMap) throws InterruptedException {
		String docKey=getDocKey(docMap);
		boolean loop=false;
		int times=0;
		do{
			try{
				elasticUtil.push(docKey, docMap);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(elasticConfig.failMaxWaitMills);
				log.error("send occur excepton: ",e);
			}
		}while(loop && times<elasticConfig.maxRetryTimes);
		
		if(loop) elasticConfig.preFailSinkSet.add(new CollectionMapper<HashMap<String,Object>>(docKey,docMap));
		return !loop;
	}
	
	/**
	 * 按集合表命名空间批量发送文档到Elastic存储
	 * @return 是否发送成功
	 * @throws InterruptedException 
	 */
	private boolean batchSend(ArrayList<HashMap<String,Object>> batchDocList) throws InterruptedException {
		ArrayList<Integer> sucIndexs=new ArrayList<Integer>();
		int len=batchDocList.size();
		boolean loop=false;
		int times=0;
		int i=0;
		do{
			try{
				HashMap<String, Object> docMap=null;
				while(i<len) {
					String docKey=getDocKey(docMap=batchDocList.get(i));
					elasticUtil.push(docKey, docMap);
					sucIndexs.add(i++);
				}
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(elasticConfig.failMaxWaitMills);
				log.error("send occur excepton: "+e.getMessage());
			}
		}while(loop && times<elasticConfig.maxRetryTimes);
		
		int indexSize=sucIndexs.size();
		for(int j=0;j<indexSize;j++) batchDocList.remove(sucIndexs.get(j)-j);
		
		for(HashMap<String, Object> docMap:batchDocList) {
			elasticConfig.preFailSinkSet.add(new CollectionMapper<HashMap<String,Object>>(getDocKey(docMap),docMap));
		}
		
		batchDocList.clear();
		return !loop;
	}
	
	/**
	 * 获取写出集合表
	 * @param doc 写出文档
	 * @return 集合表
	 */
	private String getDocKey(HashMap<String,Object> docMap) {
		String index=elasticConfig.defaultIndex;
		if(null!=elasticConfig.indexField) {
			String indexNameStr=(String)docMap.remove(elasticConfig.indexField);
			if(!isEmpty(indexNameStr)) index="/"+indexNameStr.trim().toLowerCase();
		}
		
		String type=elasticConfig.defaultType;
		if(null!=elasticConfig.typeField) {
			String typeNameStr=(String)docMap.remove(elasticConfig.typeField);
			if(!isEmpty(typeNameStr)) type="/"+typeNameStr.trim();
		}
		
		String docId="";
		if(null!=elasticConfig.idField) {
			Object obj=docMap.remove(elasticConfig.idField);
			if(null!=obj) {
				String idStr=obj.toString().trim();
				if(!idStr.isEmpty()) docId="/"+idStr;
			}
		}
		
		return new StringBuilder(index).append(type).append(docId).toString();
	}
	
	public boolean stop() {
		try {
			if(null!=elasticConfig.restClient) elasticConfig.restClient.close();
			return true;
		} catch (IOException e) {
			log.error("close RestClient occur error: {}",e);
			return false;
		}
	}
	
	/**
	 * 获取参数值
	 * @param key 参数名
	 * @param defaultValue 默认参数值
	 * @return 参数值
	 */
	private static final boolean isEmpty(String value) {
		if(null==value) return true;
		return value.trim().isEmpty();
	}
	
	/**
	 * 转换为数字类型
	 * @param value 待转换的数字对象
	 * @return 数字值
	 */
	private Number getNumber(Object value) {
		String str=value.toString().trim();
		if(str.isEmpty()) return null;
		if(CommonUtil.isInteger(str)) return Long.parseLong(str);
		return Double.parseDouble(str);
	}
	
	/**
	 * 转换为GMT+8时间戳
	 * @param value 待转换的时间对象
	 * @return GMT+8时间戳
	 */
	private String getESTimestamp(Object value) {
		Timestamp ts=null;
		if(value instanceof String) {
			String str=value.toString().trim();
			if(str.isEmpty()) return null;
			ts=Timestamp.valueOf(str);
		}else if(value instanceof Number) {
			ts=new Timestamp(((Number)value).longValue());
		}else if(value instanceof java.util.Date) {
			ts=new Timestamp(((java.util.Date)value).getTime());
		}else{
			return null;
		}
		
		String[] timeArr=BLANK_REGEX.split(ts.toString());
		return new StringBuilder(timeArr[0]).append('T').append(timeArr[1]).append(elasticConfig.timeZone).toString();
	}
}
