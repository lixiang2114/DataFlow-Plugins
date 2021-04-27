package com.df.plugin.sink.hbase.service;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.hbase.config.HbaseConfig;
import com.df.plugin.sink.hbase.dto.FieldMapper;
import com.df.plugin.sink.hbase.dto.HRecord;
import com.df.plugin.sink.hbase.util.HBaseUtil;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description Hbase服务
 */
@SuppressWarnings("unchecked")
public class HbaseService {
	/**
	 * Hbase客户端工具集
	 */
	private HBaseUtil hbaseUtil;
	
	/**
	 * 通道字段映射器
	 */
	private FieldMapper fieldMapper;
	
	/**
	 * Hbase发送器配置
	 */
	private HbaseConfig hbaseConfig;
	
	/**
	 * 批量记录表
	 */
	public ArrayList<HRecord> batchList=new ArrayList<HRecord>();
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HbaseService.class);
	
	public HbaseService(){}
	
	public HbaseService(HbaseConfig hbaseConfig){
		this.hbaseConfig=hbaseConfig;
		this.fieldMapper=hbaseConfig.fieldMapper;
		this.hbaseUtil=new HBaseUtil(hbaseConfig.maxVersion,hbaseConfig.hostList);
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws InterruptedException
	 */
	public boolean preSend() throws InterruptedException {
		if(0==hbaseConfig.preFailSinkSet.size())  return true;
		List<HRecord> failSinkList=hbaseConfig.preFailSinkSet.stream().map(e->(HRecord)e).collect(Collectors.toList());
		if(batchSend(failSinkList)) {
			hbaseConfig.preFailSinkSet.clear();
			return true;
		}
		return false;
	}
	
	/**
	 * 解析通道消息并发送到Hbase
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean parseSend(String msg) throws InterruptedException {
		if(null==msg) {
			if(batchList.isEmpty()) return null;
			return batchSend(batchList);
		}
		
		if((msg=msg.trim()).isEmpty()) return null;
		
		String[] valueArray=hbaseConfig.fieldSeparator.split(msg);
		String tabName=fieldMapper.parseValue("tabField", valueArray);
		String rowKey=fieldMapper.parseValue("rowField", valueArray);
		String family=fieldMapper.parseValue("familyField", valueArray);
		String key=fieldMapper.parseValue("keyField", valueArray);
		String valueStr=fieldMapper.parseValue("valueField", valueArray);
		if(null==tabName || tabName.isEmpty()) tabName=hbaseConfig.defaultTab;
		
		Object value=null;
		if(hbaseConfig.numFieldSet.contains(key)) {
			value=getNumber(valueStr);
		}else if(hbaseConfig.timeFieldSet.contains(key)) {
			value=getTimestamp(valueStr);
		}else{
			value=valueStr;
		}
		
		batchList.add(new HRecord(tabName,rowKey,family,key,value));
		if(batchList.size()<hbaseConfig.batchSize) return null;
		return batchSend(batchList);
	}
	
	/**
	 * 不解析通道消息直接发送到Hbase
	 * @param msg 消息内容
	 * @return 发送结果(null:未发送,true:发送成功,false:发送失败)
	 * @throws InterruptedException
	 */
	public Boolean noParseSend(String msg) throws InterruptedException {
		if(null==msg) {
			if(batchList.isEmpty()) return null;
			return batchSend(batchList);
		}
		
		if((msg=msg.trim()).isEmpty()) return null;
		
		Map<String,Map<String,Map<String,Map<String,Object>>>> tabMap=CommonUtil.jsonStrToJava(msg, HashMap.class);
		if(null==tabMap || tabMap.isEmpty()) return null;
		
		for(Entry<String,Map<String,Map<String,Map<String,Object>>>> tabEntry:tabMap.entrySet()) {
			String tabName=tabEntry.getKey();
			if(null==tabName || (tabName=tabName.trim()).isEmpty()) tabName=hbaseConfig.defaultTab;
			Map<String,Map<String,Map<String,Object>>> rowMap=tabEntry.getValue();
			if(null==rowMap || rowMap.isEmpty()) continue;
			for(Entry<String,Map<String,Map<String,Object>>> rowEntry:rowMap.entrySet()) {
				String rowKey=rowEntry.getKey();
				Map<String, Map<String, Object>> familyMap=rowEntry.getValue();
				if(null==rowKey || null==familyMap || familyMap.isEmpty() || (rowKey=rowKey.trim()).isEmpty()) continue;
				for(Entry<String, Map<String, Object>> familyEntry:familyMap.entrySet()) {
					String family=familyEntry.getKey();
					Map<String, Object> fieldMap=familyEntry.getValue();
					if(null==family || null==fieldMap || fieldMap.isEmpty() || (family=family.trim()).isEmpty()) continue;
					for(Entry<String, Object> entry:fieldMap.entrySet()) {
						String key=entry.getKey();
						Object value=entry.getValue();
						if(null==key || null==value || (key=key.trim()).isEmpty()) continue;
						if(hbaseConfig.timeFieldSet.contains(key)) value=getTimestamp(value);
						batchList.add(new HRecord(tabName,rowKey,family,key,value));
					}
				}
			}
		}
		
		if(batchList.size()<hbaseConfig.batchSize) return null;
		return batchSend(batchList);
	}
	
	/**
	 * 将批量记录发送到Hbase数据库
	 * @param batchList 批量记录集
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	private boolean batchSend(Collection<HRecord> batchCollection) throws InterruptedException {
		Map<String,Map<String,Map<String,Map<String,Object>>>> tabMap=new HashMap<String,Map<String,Map<String,Map<String,Object>>>>();
		for(HRecord record:batchCollection) {
			Map<String, Map<String, Map<String, Object>>> rowMap=tabMap.get(record.tabName);
			if(null==rowMap) tabMap.put(record.tabName, rowMap=new HashMap<String, Map<String, Map<String, Object>>>());
			Map<String, Map<String, Object>> familyMap=rowMap.get(record.rowKey);
			if(null==familyMap) rowMap.put(record.rowKey, familyMap=new HashMap<String, Map<String, Object>>());
			Map<String, Object> fieldMap=familyMap.get(record.family);
			if(null==fieldMap) familyMap.put(record.family, fieldMap=new HashMap<String, Object>());
			fieldMap.put(record.fieldKey, record.fieldValue);
		}
		
		boolean loop=false;
		int times=0;
		do{
			try{
				hbaseUtil.batchPut(tabMap, hbaseConfig.autoCreate);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(hbaseConfig.failMaxWaitMills);
				log.error("send occur excepton: ",e);
			}
		}while(loop && times<hbaseConfig.maxRetryTimes);
		
		if(loop) hbaseConfig.preFailSinkSet.addAll(batchCollection);
		batchCollection.clear();
		return !loop;
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
	 * 转换为时间戳
	 * @param value 待转换的时间对象
	 * @return 时间戳
	 */
	private Timestamp getTimestamp(Object value) {
		if(value instanceof java.util.Calendar) return new Timestamp(((java.util.Calendar)value).getTimeInMillis());
		if(value instanceof java.util.Date) return new Timestamp(((java.util.Date)value).getTime());
		if(value instanceof Number) return new Timestamp(((Number)value).longValue());
		if(value instanceof String) {
			String str=value.toString().trim();
			if(str.isEmpty()) return null;
			Timestamp ts=null;
			if(CommonUtil.isInteger(str)) {
				ts=new Timestamp(Long.parseLong(str));
			}else{
				ts=Timestamp.valueOf(str);
			}
			return ts;
		}else{
			return null;
		}
	}
	
	/**
	 * 断开Hbase连接
	 * @throws IOException
	 */
	public void stop() throws IOException {
		hbaseUtil.close();
	}
}
