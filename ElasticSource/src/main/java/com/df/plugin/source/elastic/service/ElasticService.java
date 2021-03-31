package com.df.plugin.source.elastic.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.elastic.config.ElasticConfig;
import com.df.plugin.source.elastic.util.ElasticUtil;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description Elastic服务模块
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class ElasticService {
	/**
	 * Elastic工具
	 */
	private ElasticUtil elasticUtil;
	
	/**
	 * ElasticSource配置
	 */
	private ElasticConfig elasticConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ElasticService.class);
	
	public ElasticService(){}
	
	public ElasticService(ElasticConfig elasticConfig){
		this.elasticConfig=elasticConfig;
		this.elasticUtil=new ElasticUtil(elasticConfig.restClient);
	}
	
	/**
	 * 启动ETL流程(读取预置Elastic存储数据)
	 */
	public Object startManualETLProcess(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("execute elastic source process...");
		String indexName=elasticConfig.indexName;
		HashMap<String,Object> queryParamDict=elasticConfig.queryParamDict;
		
		try{
			while(elasticConfig.flow.sourceStart) {
				int len=0;
				int counter=0;
				ArrayList<HashMap> docList=null;
				len=(docList=elasticUtil.pull(indexName, queryParamDict)).size();
				for(int i=0;i<len;i++,counter++) {
					HashMap<String,Object> docMeta=docList.get(i);
					if(null==docMeta || docMeta.isEmpty()) continue;
					HashMap<String,Object> doc=(HashMap<String,Object>)docMeta.get("_source");
					
					for(String timeField:elasticConfig.timeFieldSet) {
						Object value=doc.get(timeField);
						if(null==value) continue;
						doc.put(timeField, value.toString().replace('T', ' ').replace(elasticConfig.timeZone, ""));
					}
					
					switch(elasticConfig.outFormat) {
						case "map":
							sourceToFilterChannel.put(CommonUtil.javaToJsonStr(doc));
							break;
						case "qstr":
							StringBuilder recordQuery=new StringBuilder("");
							for(Map.Entry<String, Object> entry:doc.entrySet()) {
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
							for(Map.Entry<String, Object> entry:doc.entrySet()) {
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
				
				if(counter<elasticConfig.batchSize && !elasticConfig.realtime) break;
				queryParamDict.put("from", elasticConfig.startIndex=elasticConfig.startIndex+counter);
				if(counter>=elasticConfig.batchSize) continue;
				Thread.sleep(2000L);
			}
			
			log.info("ElasticSource plugin etl process normal exit,execute checkpoint...");
		}catch(Exception e){
			log.info("ElasticSource plugin etl process occur Error...",e);
		}finally{
			try {
				elasticConfig.refreshCheckPoint();
			} catch (IOException e1) {
				log.info("ElasticSource call refreshCheckPoint occur Error...",e1);
			}
			elasticConfig.restClient.close();
		}
		return true;
	}
	
	/**
	 * 停止ETL流程
	 */
	public boolean stopManualETLProcess(Object params) {
		log.info("stop source ETL process...");
		elasticConfig.flow.sourceStart=false;
		try {
			if(null!=elasticConfig.restClient) elasticConfig.restClient.close();
			return true;
		} catch (IOException e) {
			log.error("close RestClient occur error: {}",e);
			return false;
		}
	}
}
