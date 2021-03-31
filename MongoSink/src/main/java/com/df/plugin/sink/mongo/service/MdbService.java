package com.df.plugin.sink.mongo.service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.mongo.config.MdbConfig;
import com.df.plugin.sink.mongo.dto.CollectionMapper;
import com.df.plugin.sink.mongo.dto.CollectionWrapper;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * @author Lixiang
 * @description MDB服务
 */
@SuppressWarnings("unchecked")
public class MdbService {
	/**
	 * MongoDB配置
	 */
	private MdbConfig mdbConfig;
	
	/**
	 * 批量文档表
	 */
	public ArrayList<Document> batchList=new ArrayList<Document>();
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MdbService.class);
	
	public MdbService(){}
	
	public MdbService(MdbConfig mdbConfig){
		this.mdbConfig=mdbConfig;
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws InterruptedException
	 */
	public boolean preSend() throws InterruptedException {
		if(0==mdbConfig.preFailSinkSet.size())  return true;
		for(Object object:mdbConfig.preFailSinkSet){
			CollectionMapper<Document> collectionMapper=(CollectionMapper<Document>)object;
			collectionMapper.setMdbConfig(mdbConfig);
			if(!collectionMapper.send()) return false;
			mdbConfig.preFailSinkSet.remove(object);
		}
		return true;
	}
	
	/**
	 * 解析通道消息并发送到MDB
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean parseAndSingleSend(String msg) throws InterruptedException {
		HashMap<String,Object> docMap=new HashMap<String,Object>();
		String[] fieldValues=mdbConfig.fieldSeparator.split(msg);
		if(null==mdbConfig.fieldList || 0==mdbConfig.fieldList.length){
			for(int i=0;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}else if(mdbConfig.fieldList.length>=fieldValues.length){
			for(int i=0;i<fieldValues.length;docMap.put(mdbConfig.fieldList[i], fieldValues[i]),i++);
		}else{
			int i=0;
			for(;i<mdbConfig.fieldList.length;docMap.put(mdbConfig.fieldList[i], fieldValues[i]),i++);
			for(;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}
		
		if(docMap.isEmpty()) return null;
		
		if(null!=mdbConfig.idField){
			String docIdVal=null;
			if(!(docIdVal=docMap.getOrDefault(mdbConfig.idField, "").toString().trim()).isEmpty()) docMap.put("_id", docIdVal);
		}
		
		for(String numField:mdbConfig.numFieldSet) {
			Object value=docMap.get(numField);
			if(null==value) continue;
			docMap.put(numField, getNumber(value));
		}
		
		for(String timeField:mdbConfig.timeFieldSet) {
			Object value=docMap.get(timeField);
			if(null==value) continue;
			docMap.put(timeField, getGMTimestamp(value));
		}
		
		return singleSend(new Document(docMap));
	}
	
	/**
	 * 解析通道消息并发送到MDB
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
		String[] fieldValues=mdbConfig.fieldSeparator.split(msg);
		if(null==mdbConfig.fieldList || 0==mdbConfig.fieldList.length){
			for(int i=0;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}else if(mdbConfig.fieldList.length>=fieldValues.length){
			for(int i=0;i<fieldValues.length;docMap.put(mdbConfig.fieldList[i], fieldValues[i]),i++);
		}else{
			int i=0;
			for(;i<mdbConfig.fieldList.length;docMap.put(mdbConfig.fieldList[i], fieldValues[i]),i++);
			for(;i<fieldValues.length;docMap.put("field"+i, fieldValues[i]),i++);
		}
		
		if(docMap.isEmpty()) return null;
		
		if(null!=mdbConfig.idField){
			String docIdVal=null;
			if(!(docIdVal=docMap.getOrDefault(mdbConfig.idField, "").toString().trim()).isEmpty()) docMap.put("_id", docIdVal);
		}
		
		for(String numField:mdbConfig.numFieldSet) {
			Object value=docMap.get(numField);
			if(null==value) continue;
			docMap.put(numField, getNumber(value));
		}
		
		for(String timeField:mdbConfig.timeFieldSet) {
			Object value=docMap.get(timeField);
			if(null==value) continue;
			docMap.put(timeField, getGMTimestamp(value));
		}
		
		batchList.add(new Document(docMap));
		if(batchList.size()<mdbConfig.batchSize) return null;
		
		return batchSend(batchList);
	}
	
	/**
	 * 不解析通道消息并实时发送到MDB
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean noParseAndSingleSend(String msg) throws InterruptedException {
		HashMap<String,Object> docMap=CommonUtil.jsonStrToJava(msg, HashMap.class);
		if(null==docMap || docMap.isEmpty()) return null;
		
		if(null!=mdbConfig.idField){
			String docIdVal=null;
			if(!(docIdVal=docMap.getOrDefault(mdbConfig.idField, "").toString().trim()).isEmpty()) docMap.put("_id", docIdVal);
		}
		
		for(String timeField:mdbConfig.timeFieldSet){
			Object value=docMap.get(timeField);
			if(null==value) continue;
			docMap.put(timeField, getGMTimestamp(value));
		}
		
		return singleSend(new Document(docMap));
	}
	
	/**
	 * 不解析通道消息直接发送到MDB
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
		
		if(null!=mdbConfig.idField){
			String docIdVal=null;
			if(!(docIdVal=docMap.getOrDefault(mdbConfig.idField, "").toString().trim()).isEmpty()) docMap.put("_id", docIdVal);
		}
		
		for(String timeField:mdbConfig.timeFieldSet){
			Object value=docMap.get(timeField);
			if(null==value) continue;
			docMap.put(timeField, getGMTimestamp(value));
		}
		
		batchList.add(new Document(docMap));
		if(batchList.size()<mdbConfig.batchSize) return null;
		
		return batchSend(batchList);
	}
	
	/**
	 * 发送单个文档到MDB
	 * @param doc 文档对象
	 * @return 是否发送成功
	 * @throws InterruptedException 
	 */
	private boolean singleSend(Document doc) throws InterruptedException {
		MongoCollection<Document> collection=getCollection(doc);
		boolean loop=false;
		int times=0;
		do{
			try{
				collection.insertOne(doc);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(mdbConfig.failMaxWaitMills);
				log.error("send occur excepton: ",e);
			}
		}while(loop && times<mdbConfig.maxRetryTimes);
		
		if(loop) mdbConfig.preFailSinkSet.add(new CollectionMapper<Document>(doc,collection));
		return !loop;
	}
	
	/**
	 * 按集合表命名空间批量发送文档到MDB
	 * @return 是否发送成功
	 * @throws InterruptedException 
	 */
	private boolean batchSend(ArrayList<Document> batchDocList) throws InterruptedException {
		HashMap<CollectionWrapper<Document>,ArrayList<Document>> collMap=getBatchCollectionMap(batchDocList);
		if(null==collMap || collMap.isEmpty()) return false;
		boolean finalSuccess=true;
		
		Set<Entry<CollectionWrapper<Document>, ArrayList<Document>>> entrys=collMap.entrySet();
		for(Entry<CollectionWrapper<Document>, ArrayList<Document>> entry:entrys) {
			MongoCollection<Document> collection=entry.getKey().collection;
			ArrayList<Document> docList=entry.getValue();
			boolean loop=false;
			int times=0;
			do{
				try{
					collection.insertMany(docList);
					loop=false;
				}catch(Exception e) {
					times++;
					loop=true;
					Thread.sleep(mdbConfig.failMaxWaitMills);
					log.error("send occur excepton: ",e);
				}
			}while(loop && times<mdbConfig.maxRetryTimes);
			if(loop) {
				finalSuccess=false;
				mdbConfig.preFailSinkSet.add(new CollectionMapper<Document>(docList,collection));
			}
		}
		
		batchDocList.clear();
		return finalSuccess;
	}
	
	/**
	 * 获取写出集合表字典
	 * @param docList 写出文档列表
	 * @return 集合表字典
	 */
	private HashMap<CollectionWrapper<Document>,ArrayList<Document>> getBatchCollectionMap(ArrayList<Document> docList) {
		if(null==docList) return null;
		
		HashMap<CollectionWrapper<Document>,ArrayList<Document>> collMap=
				new HashMap<CollectionWrapper<Document>,ArrayList<Document>>();
		
		for(Document doc:docList) {
			CollectionWrapper<Document> collectionWrapper=getCollectionWrapper(doc);
			ArrayList<Document> tmpList=collMap.get(collectionWrapper);
			if(null==tmpList) collMap.put(collectionWrapper, tmpList=new ArrayList<Document>());
			tmpList.add(doc);
		}
		
		return collMap;
	}
	
	/**
	 * 获取写出集合表
	 * @param doc 写出文档
	 * @return 集合表
	 */
	private MongoCollection<Document> getCollection(Document doc) {
		MongoDatabase database=mdbConfig.defaultDatabase;
		if(null!=mdbConfig.dbField) {
			String dbNameStr=(String)doc.remove(mdbConfig.dbField);
			if(!isEmpty(dbNameStr)) {
				database=mdbConfig.mongoClient.getDatabase(dbNameStr.trim());
			}
		}
		
		MongoCollection<Document> collection=mdbConfig.defaultCollection;
		if(null!=mdbConfig.tabField) {
			String tabNameStr=(String)doc.remove(mdbConfig.tabField);
			if(!isEmpty(tabNameStr)) {
				collection=database.getCollection(tabNameStr.trim(),Document.class);
			}
		}
		
		return collection;
	}
	
	/**
	 * 获取写出集合表包装器
	 * @param doc 写出文档
	 * @return 集合表包装器
	 */
	private CollectionWrapper<Document> getCollectionWrapper(Document doc) {
		String dbName=mdbConfig.defaultDB;
		MongoDatabase database=mdbConfig.defaultDatabase;
		if(null!=mdbConfig.dbField) {
			String dbNameStr=(String)doc.remove(mdbConfig.dbField);
			if(!isEmpty(dbNameStr)) {
				dbName=dbNameStr.trim();
				database=mdbConfig.mongoClient.getDatabase(dbName);
			}
		}
		
		String tabName=mdbConfig.defaultTab;
		MongoCollection<Document> collection=mdbConfig.defaultCollection;
		if(null!=mdbConfig.tabField) {
			String tabNameStr=(String)doc.remove(mdbConfig.tabField);
			if(!isEmpty(tabNameStr)) {
				tabName=tabNameStr.trim();
				collection=database.getCollection(tabName,Document.class);
			}
		}
		
		return new CollectionWrapper<Document>(dbName,tabName,collection);
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
	private Timestamp getGMTimestamp(Object value) {
		if(value instanceof java.util.Date) return new Timestamp(((java.util.Date)value).getTime()+mdbConfig.timeZoneMillis);
		if(value instanceof Number) return new Timestamp(((Number)value).longValue()+mdbConfig.timeZoneMillis);
		if(value instanceof String) {
			String str=value.toString().trim();
			if(str.isEmpty()) return null;
			Timestamp ts=Timestamp.valueOf(str);
			ts.setTime(ts.getTime()+mdbConfig.timeZoneMillis);
			return ts;
		}else{
			return null;
		}
	}
}
