package com.df.plugin.sink.sql.service;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.sql.config.SqlConfig;
import com.df.plugin.sink.sql.dto.RecordMapper;
import com.df.plugin.sink.sql.dto.SQLMapper;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description SQL服务模块
 */
@SuppressWarnings("unchecked")
public class SqlService {
	/**
	 * SQL客户端配置
	 */
	private SqlConfig sqlConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(SqlService.class);
	
	/**
	 * 批量记录字典
	 */
	private ConcurrentHashMap<SQLMapper, ArrayList<Object[]>> batchMap=new ConcurrentHashMap<SQLMapper, ArrayList<Object[]>>();
	
	public SqlService(){}
	
	public SqlService(SqlConfig sqlConfig){
		this.sqlConfig=sqlConfig;
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws Exception
	 */
	public boolean preSend() throws Exception {
		if(sqlConfig.preFailSinkSet.isEmpty())return true;
		for(Object object:sqlConfig.preFailSinkSet){
			RecordMapper recordMapper=(RecordMapper)object;
			recordMapper.sqlConfig=sqlConfig;
			if(!recordMapper.send()) return false;
			sqlConfig.preFailSinkSet.remove(object);
		}
		return true;
	}
	
	/**
	 * 解析通道消息并发送到SQL数据库
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws Exception
	 */
	public Boolean parseAndSingleSend(String msg) throws Exception {
		List<Object> valueList=Arrays.stream(sqlConfig.fieldSeparator.split(msg)).collect(Collectors.toList());
		SQLMapper sqlMapper=sqlConfig.getSqlMapper(getTableFullName(valueList));
		if(null==sqlMapper) return null;
		return singleSend(valueList,sqlMapper);
	}
	
	/**
	 * 解析通道消息并发送到SQL数据库
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws Exception
	 */
	public Boolean parseAndBatchSend(String msg) throws Exception {
		if(null==msg) {
			if(0==getBatchSize()) return null;
			return batchSend(batchMap);
		}
		
		if((msg=msg.trim()).isEmpty()) return null;
		
		List<Object> valueList=Arrays.stream(sqlConfig.fieldSeparator.split(msg)).collect(Collectors.toList());
		SQLMapper sqlMapper=sqlConfig.getSqlMapper(getTableFullName(valueList));
		if(null==sqlMapper) return null;
		
		if(valueList.size()<sqlMapper.fieldMap.size()) {
			log.error("call parseAndBatchSend occur error: record.length<sqlConfig.fieldMap.size");
			throw new RuntimeException("call parseAndBatchSend occur error: record.length<sqlConfig.fieldMap.size");
		}
		
		ArrayList<Object[]> batchList=batchMap.get(sqlMapper);
		if(null==batchList) batchMap.put(sqlMapper, batchList=new ArrayList<Object[]>());
		batchList.add(valueList.toArray());
		
		if(sqlConfig.batchSize>getBatchSize()) return null;
		return batchSend(batchMap);
	}
	
	/**
	 * 不解析通道消息并实时发送到SQL数据库
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws Exception
	 */
	public Boolean noParseAndSingleSend(String msg) throws Exception {
		HashMap<String,Object> recordMap=CommonUtil.jsonStrToJava(msg, HashMap.class);
		SQLMapper sqlMapper=sqlConfig.getSqlMapper(getTableFullName(recordMap));
		if(null==sqlMapper) return null;
		
		List<Object> valueList=new ArrayList<Object>();
		for(Iterator<Entry<String, Class<?>>> its=sqlMapper.fieldMap.entrySet().iterator();its.hasNext();valueList.add(recordMap.get(its.next().getKey())));
		return singleSend(valueList,sqlMapper);
	}
	
	/**
	 * 不解析通道消息直接发送到SQL数据库
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws Exception
	 */
	public Boolean noParseAndBatchSend(String msg) throws Exception {
		if(null==msg) {
			if(0==getBatchSize()) return null;
			return batchSend(batchMap);
		}
		
		if((msg=msg.trim()).isEmpty()) return null;
		
		HashMap<String,Object> recordMap=CommonUtil.jsonStrToJava(msg, HashMap.class);
		SQLMapper sqlMapper=sqlConfig.getSqlMapper(getTableFullName(recordMap));
		if(null==sqlMapper) return null;
		
		if(recordMap.size()<sqlMapper.fieldMap.size()) {
			log.error("call parseAndBatchSend occur error: record.length<sqlConfig.fieldMap.size");
			throw new RuntimeException("call parseAndBatchSend occur error: record.length<sqlConfig.fieldMap.size");
		}
		
		ArrayList<Object[]> batchList=batchMap.get(sqlMapper);
		if(null==batchList) batchMap.put(sqlMapper, batchList=new ArrayList<Object[]>());
		
		List<Object> valueList=new ArrayList<Object>();
		for(Iterator<Entry<String, Class<?>>> its=sqlMapper.fieldMap.entrySet().iterator();its.hasNext();valueList.add(recordMap.get(its.next().getKey())));
		batchList.add(valueList.toArray());
		
		if(sqlConfig.batchSize>getBatchSize()) return null;
		return batchSend(batchMap);
	}
	
	/**
	 * 发送单条记录到SQL数据库
	 * @param record 记录对象
	 * @param sqlMapper SQL映射器
	 * @return 是否发送成功
	 * @throws Exception 
	 */
	private boolean singleSend(List<Object> valueList,SQLMapper sqlMapper) throws Exception {
		LinkedHashMap<String,Class<?>> fieldMap=sqlMapper.fieldMap;
		
		if(valueList.size()<fieldMap.size()) {
			log.error("call singleSend occur error: record.length<sqlConfig.fieldMap.size");
			throw new RuntimeException("call singleSend occur error: record.length<sqlConfig.fieldMap.size");
		}
		
		PreparedStatement pstat=sqlMapper.getStatement();
		Iterator<Map.Entry<String,Class<?>>> typeList=fieldMap.entrySet().iterator();
		for(int i=0;typeList.hasNext();pstat.setObject(i+1, CommonUtil.transferType(valueList.get(i), typeList.next().getValue())),i++);
		
		boolean loop=false;
		int times=0;
		do{
			try{
				if(0>=pstat.executeUpdate()) throw new RuntimeException("call singleSend failure: count<=0");
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(sqlConfig.failMaxWaitMills);
				log.error("call singleSend excepton: ",e);
			}
		}while(loop && times<sqlConfig.maxRetryTimes);
		
		if(loop) sqlConfig.preFailSinkSet.add(new RecordMapper(valueList.toArray(),sqlMapper));
		pstat.clearParameters();
		return !loop;
	}
	
	/**
	 * 批量发送记录到SQL数据库
	 * @param recordList 记录列表
	 * @return 是否发送成功
	 * @throws Exception 
	 */
	private boolean batchSend(Map<SQLMapper, ArrayList<Object[]>> batchMap) throws Exception {
		boolean finalSuccess=true;
		Set<Entry<SQLMapper, ArrayList<Object[]>>> entrys=batchMap.entrySet();
		for(Entry<SQLMapper, ArrayList<Object[]>> entry:entrys) {
			SQLMapper sqlMapper=entry.getKey();
			ArrayList<Object[]> recordList=entry.getValue();
			PreparedStatement pstat=sqlMapper.getStatement();
			LinkedHashMap<String,Class<?>> fieldMap=sqlMapper.fieldMap;
			
			for(Object[] record:recordList) {
				Iterator<Map.Entry<String,Class<?>>> typeList=fieldMap.entrySet().iterator();
				for(int i=0;typeList.hasNext();pstat.setObject(i+1, CommonUtil.transferType(record[i], typeList.next().getValue())),i++);
				pstat.addBatch();
			}
			
			boolean loop=false;
			int times=0;
			do{
				try{
					int[] counts=pstat.executeBatch();
					if(counts.length!=recordList.size()) throw new RuntimeException("executeBatch failure: counts.length!=recordList.size");
					for(int count:counts) if(count<=0) throw new RuntimeException("executeBatch failure: count<=0");
					loop=false;
				}catch(Exception e) {
					times++;
					loop=true;
					Thread.sleep(sqlConfig.failMaxWaitMills);
					log.error("call batchSend excepton: ",e);
				}
			}while(loop && times<sqlConfig.maxRetryTimes);
			
			if(loop) {
				finalSuccess=false;
				sqlConfig.preFailSinkSet.add(new RecordMapper(recordList,sqlMapper));
			}
			
			pstat.clearParameters();
			pstat.clearBatch();
		}
		
		batchMap.clear();
		return finalSuccess;
	}
	
	/**
	 * 获取写出集合表包装器
	 * @param doc 写出文档
	 * @return 集合表包装器
	 */
	private String getTableFullName(List<Object> record) {
		if(null==record || record.isEmpty()) return null;
		
		int dbIndex=-1;
		int tabIndex=-1;
		boolean removeDB=false;
		boolean removeTab=false;
		String dbName=sqlConfig.defaultDB;
		String tabName=sqlConfig.defaultTab;
		
		if(null!=sqlConfig.dbIndex) {
			dbIndex=sqlConfig.dbIndex.intValue();
			String dbNameStr=(String)record.get(dbIndex);
			if(!isEmpty(dbNameStr)) {
				removeDB=true;
				dbName=dbNameStr.trim();
			}
		}
		
		if(null!=sqlConfig.tabIndex) {
			tabIndex=sqlConfig.tabIndex.intValue();
			String tabNameStr=(String)record.get(tabIndex);
			if(!isEmpty(tabNameStr)) {
				removeTab=true;
				tabName=tabNameStr.trim();
			}
		}
		
		if(dbIndex<tabIndex) {
			if(removeTab) {
				record.remove(tabIndex);
				if(removeDB) record.remove(dbIndex);
			}else{
				if(removeDB) record.remove(dbIndex);
			}
		}else{
			if(removeDB) {
				record.remove(dbIndex);
				if(removeTab) record.remove(tabIndex);
			}else{
				if(removeTab) record.remove(tabIndex);
			}
		}
		
		return new StringBuilder(dbName).append(".").append(tabName).toString();
	}
	
	/**
	 * 获取写出集合表包装器
	 * @param doc 写出文档
	 * @return 集合表包装器
	 */
	private String getTableFullName(Map<String,Object> recordMap) {
		if(null==recordMap || recordMap.isEmpty()) return null;
		
		String dbName=sqlConfig.defaultDB;
		if(null!=sqlConfig.dbField) {
			String dbNameStr=(String)recordMap.remove(sqlConfig.dbField);
			if(!isEmpty(dbNameStr)) {
				dbName=dbNameStr.trim();
			}
		}
		
		String tabName=sqlConfig.defaultTab;
		if(null!=sqlConfig.tabField) {
			String tabNameStr=(String)recordMap.remove(sqlConfig.tabField);
			if(!isEmpty(tabNameStr)) {
				tabName=tabNameStr.trim();
			}
		}
		
		return new StringBuilder(dbName).append(".").append(tabName).toString();
	}
	
	/**
	 * 获取参数值
	 * @param key 参数名
	 * @param defaultValue 默认参数值
	 * @return 参数值
	 */
	private final int getBatchSize() {
		int sum=0;
		for(ArrayList<Object[]> list:batchMap.values()) sum+=list.size();
		return sum;
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
}
