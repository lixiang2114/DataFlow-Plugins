package com.df.plugin.sink.mongo.dto;

import com.mongodb.client.MongoCollection;

/**
 * @author Lixiang
 * @description 集合表包装器
 */
@SuppressWarnings({"unchecked"})
public class CollectionWrapper<E> {
	/**
	 * 命名空间
	 */
	private String nameSpace;
	
	/**
	 * 集合表对象
	 */
	public MongoCollection<E> collection;
	
	public CollectionWrapper(String dbName,String tabName,MongoCollection<E> collection) {
		this.collection=collection;
		this.nameSpace=dbName+"."+tabName;
	}
	
	@Override
	public String toString() {
		return nameSpace;
	}

	@Override
	public int hashCode() {
		return nameSpace.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(this==obj) return true;
		if(!(obj instanceof CollectionWrapper)) return false;
		CollectionWrapper<E> cw=(CollectionWrapper<E>)obj;
		return nameSpace.equals(cw.nameSpace);
	}
}
