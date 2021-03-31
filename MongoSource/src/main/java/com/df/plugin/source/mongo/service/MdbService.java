package com.df.plugin.source.mongo.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.mongo.config.MdbConfig;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

/**
 * @author Lixiang
 * @description MDB服务模块
 */
@SuppressWarnings("unchecked")
public class MdbService {
	/**
	 * MongoSource配置
	 */
	private MdbConfig mdbConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MdbService.class);
	
	public MdbService(){}
	
	public MdbService(MdbConfig mdbConfig){
		this.mdbConfig=mdbConfig;
	}
	
	/**
	 * 停止ETL流程
	 */
	public Boolean stopManualETLProcess(Object params) {
		log.info("stop source ETL process...");
		mdbConfig.flow.sourceStart=false;
		return true;
	}

	/**
	 * 启动ETL流程(读取预置MDB数据库数据)
	 */
	public Object startManualETLProcess(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("execute mongo source process...");
		ArrayList<BasicDBObject> pipeList=mdbConfig.pipeLine;
		MongoCollection<Document> collection=mdbConfig.mongoCollection;
		try{
			while(mdbConfig.flow.sourceStart){
				int counter=0;
				MongoCursor<Document> docList=collection.aggregate(pipeList).iterator();
				for(;docList.hasNext();counter++) {
					Document doc=docList.next();
					doc.put("_id", doc.get("_id").toString());
					switch(mdbConfig.outFormat) {
						case "map":
							sourceToFilterChannel.put(doc.toJson());
							break;
						case "qstr":
							StringBuilder recordQuery=new StringBuilder("");
							for(Entry<String, Object> entry:doc.entrySet()) {
								Object value=entry.getValue();
								if(null==value) value="null";
								if(java.util.Date.class.isAssignableFrom(value.getClass())) {
									recordQuery.append(entry.getKey().trim()).append("=").append(CommonUtil.transferType(value, String.class)).append("&");
								}else{
									recordQuery.append(entry.getKey().trim()).append("=").append(value).append("&");
								}
							}
							if(0==recordQuery.length()) continue;
							sourceToFilterChannel.put(recordQuery.deleteCharAt(recordQuery.length()-1).toString());
							break;
						default:
							StringBuilder recordValues=new StringBuilder("");
							for(Entry<String, Object> entry:doc.entrySet()) {
								Object value=entry.getValue();
								if(null==value) value="null";
								if(java.util.Date.class.isAssignableFrom(value.getClass())) {
									recordValues.append(CommonUtil.transferType(value, String.class)).append(",");
								}else{
									recordValues.append(value).append(",");
								}
							}
							if(0==recordValues.length()) continue;
							sourceToFilterChannel.put(recordValues.deleteCharAt(recordValues.length()-1).toString());
					}
				}
				
				if(counter<mdbConfig.batchSize && !mdbConfig.realtime) break;
				pipeList.get(pipeList.size()-2).put("$skip",mdbConfig.startIndex=mdbConfig.startIndex+counter);
				if(counter>=mdbConfig.batchSize) continue;
				Thread.sleep(2000L);
			}
			
			log.info("MongoSource plugin etl process normal exit,execute checkpoint...");
		}catch(Exception e){
			log.error("MongoSource plugin etl process occur Error...",e);
		}finally{
			try {
				mdbConfig.refreshCheckPoint();
			} catch (IOException e) {
				log.error("MongoSource call refreshCheckPoint occur Error...",e);
			}
			mdbConfig.mongoClient.close();
		}
		return true;
	}
}
