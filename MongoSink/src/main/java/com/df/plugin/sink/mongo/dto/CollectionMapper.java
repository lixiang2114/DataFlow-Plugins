package com.df.plugin.sink.mongo.dto;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.mongo.config.MdbConfig;
import com.mongodb.client.MongoCollection;

/**
 * @author Lixiang
 * @description 集合映射器
 * @param <E>
 */
public class CollectionMapper<E> {
	/**
	 * 集合表数据
	 */
	private List<E> docList;
	
	/**
	 * MongoDB配置
	 */
	private MdbConfig mdbConfig;

	/**
	 * 集合表对象
	 */
	private MongoCollection<E> collection;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(CollectionMapper.class);
	
	public CollectionMapper(E doc,MongoCollection<E> collection) {
		this.docList=new ArrayList<E>();
		this.collection=collection;
		this.docList.add(doc);
	}
	
	public CollectionMapper(List<E> docList,MongoCollection<E> collection) {
		this.collection=collection;
		this.docList=docList;
	}
	
	public void setMdbConfig(MdbConfig mdbConfig) {
		this.mdbConfig = mdbConfig;
	}

	/**
	 * 发送数据
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	public boolean send() throws InterruptedException {
		if(null==collection || null==docList || docList.isEmpty()) return false;
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
				log.error("send occur excepton: "+e.getMessage());
			}
		}while(loop && times<mdbConfig.maxRetryTimes);
		return !loop;
	}
	
	public void clear() {
		if(null==docList) return;
		docList.clear();
	}
}
