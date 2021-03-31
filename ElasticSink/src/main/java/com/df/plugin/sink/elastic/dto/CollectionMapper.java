package com.df.plugin.sink.elastic.dto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.elastic.config.ElasticConfig;
import com.df.plugin.sink.elastic.util.ElasticUtil;

/**
 * @author Lixiang
 * @description 集合映射器
 * @param <E>
 */
public class CollectionMapper<E> {
	/**
	 * 文档
	 */
	private E doc;
	
	/**
	 * 文档键
	 */
	private String docKey;
	
	/**
	 * Elastic服务工具
	 */
	private ElasticUtil elasticUtil;
	
	/**
	 * Elastic配置
	 */
	private ElasticConfig elasticConfig;

	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(CollectionMapper.class);
	
	public CollectionMapper(String docKey,E document) {
		this.doc=document;
		this.docKey=docKey;
	}
	
	public void setElasticConfig(ElasticConfig elasticConfig) {
		this.elasticConfig = elasticConfig;
	}

	/**
	 * 发送数据
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	public boolean send() throws InterruptedException {
		if(null==docKey || null==doc || docKey.trim().isEmpty()) return false;
		boolean loop=false;
		int times=0;
		do{
			try{
				elasticUtil.push(docKey, doc);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(elasticConfig.failMaxWaitMills);
				log.error("send occur excepton: "+e.getMessage());
			}
		}while(loop && times<elasticConfig.maxRetryTimes);
		return !loop;
	}
}
