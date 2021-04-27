package com.df.plugin.source.ma.hbase.service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.ma.hbase.config.HbaseConfig;
import com.df.plugin.source.ma.hbase.util.HBaseUtil;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description Hbase分布式存储服务
 */
@SuppressWarnings("unchecked")
public class HbaseService {
	/**
	 * Hbase客户端工具集
	 */
	private HBaseUtil hbaseUtil;
	
	/**
	 * Hbase分布式存储客户端配置
	 */
	private HbaseConfig hbaseConfig;
	
	/**
	 * 对应LinkedHashMap最后一个键值对的字段
	 */
	private static Field linkedHashMapTailField;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HbaseService.class);
	
	public HbaseService(){}
	
	public HbaseService(HbaseConfig hbaseConfig){
		this.hbaseConfig=hbaseConfig;
		this.hbaseUtil=new HBaseUtil(hbaseConfig.hostList);
		try {
			linkedHashMapTailField=LinkedHashMap.class.getDeclaredField("tail");
			linkedHashMapTailField.setAccessible(true);
		} catch (Exception e) {
			log.info("obtain last entry occur error...",e);
		}
	}
	
	/**
	 * 停止离线ETL流程
	 */
	public Boolean stopManualETLProcess(Object params) {
		log.info("stop manual ETL process...");
		hbaseConfig.flow.sourceStart=false;
		return true;
	}
	
	/**
	 * 启动ETL流程(读取离线Hbase数据库数据)
	 */
	public Object startManualETLProcess(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("execute mongo source process...");
		
		int batchSize=hbaseConfig.batchSize+1;
		String targetTab=hbaseConfig.targetTab;
		String separator=hbaseConfig.fieldSeparator;
		
		try{
			boolean finished=false;
			while(hbaseConfig.flow.sourceStart){
				LinkedHashMap<String, Map<String, Map<String, Object>>> rowMap=hbaseUtil.pageGet(targetTab, hbaseConfig.startRowKey, batchSize);
				String lastKey=((Entry<String,Object>)linkedHashMapTailField.get(rowMap)).getKey();
				if(batchSize>rowMap.size()){
					finished=true;
				}else{
					rowMap.remove(lastKey);
				}
				
				if("map".equalsIgnoreCase(hbaseConfig.outFormat)) {
					Map<String,Object> tabMap=Collections.singletonMap(hbaseConfig.targetTab, rowMap);
					sourceToFilterChannel.put(CommonUtil.javaToJsonStr(tabMap));
				}else{
					for(Entry<String, Map<String, Map<String, Object>>> rowEntry:rowMap.entrySet()) {
						String rowKey=rowEntry.getKey();
						for(Entry<String, Map<String, Object>> familyEntry:rowEntry.getValue().entrySet()) {
							String family=familyEntry.getKey();
							for(Entry<String, Object> fieldEntry:familyEntry.getValue().entrySet()) {
								String record=new StringBuilder(hbaseConfig.targetTab)
										.append(separator).append(rowKey)
										.append(separator).append(family)
										.append(separator).append(fieldEntry.getKey())
										.append(separator).append(CommonUtil.transferType(fieldEntry.getValue(), String.class))
										.toString();
								sourceToFilterChannel.put(record);
							}
						}
					}
				}
				
				hbaseConfig.startRowKey=lastKey;
				if(finished) break;
			}
			
			log.info("HbaseManual plugin etl process normal exit,execute checkpoint...");
		}catch(Exception e){
			log.error("HbaseManual plugin etl process occur Error...",e);
		}finally{
			try {
				hbaseConfig.refreshCheckPoint();
			} catch (IOException e) {
				log.error("HbaseManual call refreshCheckPoint occur Error...",e);
			}
			hbaseUtil.close();
		}
		return true;
	}
}
