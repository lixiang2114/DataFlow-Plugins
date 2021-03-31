package com.df.plugin.sink.flow.service;

/**
 * @author Lixiang
 * @description 发送规则接口
 */
public interface SendRule {
	/**
	 * 获取流程索引表
	 * @param message 消息记录
	 * @param itemList 目标项表
	 * @return
	 */
	public Object[] main(String message,String[] itemList);
}
