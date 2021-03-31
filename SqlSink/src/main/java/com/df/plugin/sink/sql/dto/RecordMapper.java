package com.df.plugin.sink.sql.dto;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.sql.config.SqlConfig;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description 记录映射器
 */
@SuppressWarnings("unchecked")
public class RecordMapper {
	/**
	 * SQL数据库配置
	 */
	public SqlConfig sqlConfig;
	
	/**
	 * SQL映射器
	 */
	public SQLMapper sqlMapper;
	
	/**
	 * 记录字段值列表
	 */
	public ArrayList<Object[]> recordList;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(RecordMapper.class);
	
	public RecordMapper(Object[] record,SQLMapper sqlMapper) {
		this.recordList=new ArrayList<Object[]>();
		this.sqlMapper=sqlMapper;
		this.recordList.add(record);
	}
	
	public RecordMapper(ArrayList<Object[]> recordList,SQLMapper sqlMapper) {
		this.recordList=recordList;
		this.sqlMapper=sqlMapper;
	}
	
	/**
	 * 发送数据
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	public boolean send() throws Exception {
		if(null==sqlMapper || null==recordList || recordList.isEmpty()) return false;
		
		PreparedStatement pstat=sqlMapper.getStatement();
		LinkedHashMap<String,Class<?>> fieldMap=sqlMapper.fieldMap;
		try{
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
					if(counts.length!=recordList.size()) throw new RuntimeException("call pre-send failure: counts.length!=recordList.size");
					for(int count:counts) if(count<=0) throw new RuntimeException("call pre-send failure: count<=0");
					loop=false;
				}catch(Exception e) {
					times++;
					loop=true;
					Thread.sleep(sqlConfig.failMaxWaitMills);
					log.error("call send excepton: ",e);
				}
			}while(loop && times<sqlConfig.maxRetryTimes);
			return !loop;
		}finally{
			pstat.clearParameters();
			pstat.clearBatch();
		}
	}
	
	public void clear() {
		if(null==recordList) return;
		recordList.clear();
	}
}
