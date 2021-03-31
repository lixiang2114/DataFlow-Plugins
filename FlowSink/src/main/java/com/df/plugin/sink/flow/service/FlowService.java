package com.df.plugin.sink.flow.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.flow.config.FlowConfig;
import com.df.plugin.sink.flow.consts.RuleType;
import com.df.plugin.sink.flow.dto.FlowMapper;
import com.df.plugin.sink.flow.util.DyScriptUtil;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description 流程服务模块
 */
@SuppressWarnings("unchecked")
public class FlowService {
	/**
	 * 转存目标列表长度
	 */
	private int flowNum;
	
	/**
	 * 消息计数器
	 */
	private int counter=-1;
	
	/**
	 * 流程发送器配置
	 */
	private FlowConfig flowConfig;
	
	/**
	 * 随机算法生成器
	 */
	private Random random=new Random();
	
	/**
	 * 转存流程列表
	 */
	private ArrayList<FlowMapper> targetFlowList;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FlowService.class);
	
	public FlowService(){}
	
	public FlowService(FlowConfig flowConfig){
		this.flowConfig=flowConfig;
		this.flowNum=(this.targetFlowList=flowConfig.targetFlowList).size();
	}
	
	/**
	 * 复制并发送到每一个管道
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean repPipeLine(String message) {
		try {
			for(int i=0;i<flowNum;targetFlowList.get(i++).writeMessage(message));
			return true;
		} catch (Exception e) {
			log.error("flow sink rep pipeLine process running error...",e);
			return false;
		}
	}
	
	/**
	 * 根据字段名或索引发送到指定管道组
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean fieldPipeLine(String message) {
		String itemValues=null;
		String itemKey=flowConfig.itemKey;
		if(RuleType.key==flowConfig.ruleType) {
			HashMap<String,Object> recordDict=CommonUtil.jsonStrToJava(message, HashMap.class);
			itemValues=(String)recordDict.remove(itemKey);
			message=CommonUtil.javaToJsonStr(recordDict);
		}else{
			int itemIndex=Integer.parseInt(itemKey);
			String[] fieldValues=flowConfig.fieldRegex.split(message);
			itemValues=fieldValues[itemIndex];
			StringBuilder builder=new StringBuilder();
			for(int i=0;i<fieldValues.length;i++) {
				if(i==itemIndex) continue;
				builder.append(flowConfig.fieldRegex.pattern()).append(fieldValues[i]);
			}
			message=builder.deleteCharAt(0).toString();
		}
		
		if(null==itemValues || itemValues.isEmpty()) {
			log.error("field pipeLine process running error: flow name not found by field rule...");
			return false;
		}
		
		String[] targetItems=flowConfig.targetItems;
		String[] items=flowConfig.itemRegex.split(itemValues);
		ArrayList<Integer> indexList=new ArrayList<Integer>();
		for(int i=0;i<items.length;i++) {
			for(int j=0;j<flowNum;j++) {
				if(items[i].equals(targetItems[j])) {
					indexList.add(j);
					break;
				}
			}
		}
		
		int len=indexList.size();
		if(0==len) {
			log.error("flow sink field pipeLine process running error: indexList is Empty...");
			return false;
		}
		
		if(len<items.length) {
			log.warn("flow sink field pipeLine process running error: indexList length is less than items length...");
		}
		
		try {
			for(int i=0;i<len;targetFlowList.get(indexList.get(i++)).writeMessage(message));
			return true;
		} catch (Exception e) {
			log.error("flow sink field pipeLine process running error...",e);
			return false;
		}
	}
	
	/**
	 * 根据哈希求模索引发送到指定管道
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean hashPipeLine(String message) {
		try {
			targetFlowList.get(Math.abs(message.hashCode())%flowNum).writeMessage(message);
			return true;
		} catch (Exception e) {
			log.error("flow sink hash pipeLine process running error...",e);
			return false;
		}
	}
	
	/**
	 * 根据轮训算法分发到指定管道
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean robinPipeLine(String message) {
		this.counter=++counter>flowNum?1:counter;
		try {
			targetFlowList.get(counter%flowNum).writeMessage(message);
			return true;
		} catch (Exception e) {
			log.error("flow sink robin pipeLine process running error...",e);
			return false;
		}
	}
	
	/**
	 * 根据随机算法分发到指定管道
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean randomPipeLine(String message) {
		try {
			targetFlowList.get(random.nextInt(flowNum)).writeMessage(message);
			return true;
		} catch (Exception e) {
			log.error("flow sink random pipeLine process running error...",e);
			return false;
		}
	}
	
	/**
	 * 根据自定义算法分发到指定管道组
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean customPipeLine(String message) {
		Object result=null;
		try {
			result = DyScriptUtil.execFunc(flowConfig.mainClass, flowConfig.mainMethod,message,flowConfig.targetItems);
		} catch (Exception e) {
			log.error("flow sink custom pipeLine process running error...",e);
			throw new RuntimeException("flow sink custom pipeLine process running error...",e);
		}
		
		if(null==result) return true;
		Object[] msgAndIndex=(Object[])result;
		if(2>msgAndIndex.length) return false;
		
		message=(String)msgAndIndex[0];
		int[] indexs=(int[])msgAndIndex[1];
		
		if(null==message || null==indexs || 0==indexs.length) return false;
		for(int i=0;i<indexs.length;i++) if(0>indexs[i]) indexs[i]=0;
		
		try {
			for(int i=0;i<indexs.length;targetFlowList.get(indexs[i++]).writeMessage(message));
			return true;
		} catch (Exception e) {
			log.error("flow sink custom pipeLine process running error...",e);
			return false;
		}
	}
	
	/**
	 * 停止文件流进程
	 * @throws IOException
	 */
	public void stop() throws IOException {
		int len=targetFlowList.size();
		for(int i=0;i<len;targetFlowList.get(i++).closeFileStream());
	}
}