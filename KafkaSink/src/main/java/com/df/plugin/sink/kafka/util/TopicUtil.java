package com.df.plugin.sink.kafka.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;

/**
 * @author Lixiang
 * @description KafKa主题工具
 */
public class TopicUtil {
	/**
	 * KafKa管理客户端
	 */
	private AdminClient adminClient;

	public TopicUtil(AdminClient adminClient) {
		this.adminClient=adminClient;
	}
	
	/**
	 * 创建主题
	 * @param topicName 主题名称
	 * @throws Exception 
	 */
	public void createTopic(String topicName) throws Exception {
		createTopic(topicName,1,1);
	}
	
	/**
	 * 创建主题
	 * @param topicName 主题名称
	 * @param replicasNum 副本数量
	 * @throws Exception 
	 */
	public void createTopic(String topicName,int replicasNum) throws Exception {
		createTopic(topicName,1,replicasNum);
	}
	
	/**
	 * 创建主题
	 * @param topicName 主题名称
	 * @param partitionNum 分区数量
	 * @param replicasNum 副本数量
	 * @throws Exception 
	 */
	public void createTopic(String topicName,Integer partitionNum,int replicasNum) throws Exception {
		adminClient.createTopics(Collections.singleton(new NewTopic(topicName,partitionNum,(short)replicasNum))).all().get();
	}
	
	/**
	 * 删除主题
	 * @param topicName 主题名称
	 * @throws Exception 
	 */
	public void deleteTopic(String topicName) throws InterruptedException, ExecutionException{
		adminClient.deleteTopics(Collections.singleton(topicName)).all().get();
	}
	
	/**
	 * 修改主题分区数量
	 * @param topicName 主题名称
	 * @param partitionNum 分区数量
	 * @throws Exception 
	 */
	public void alterTopic(String topicName,Integer partitionNum) throws Exception {
		Map<String, NewPartitions> newPartitions=new HashMap<String, NewPartitions>();
		newPartitions.put(topicName, NewPartitions.increaseTo(partitionNum));
		CreatePartitionsResult result=adminClient.createPartitions(newPartitions);
		result.all().get();
	}
	
	/**
	 * 获取指定的主题
	 * @param topic 主题名称
	 * @param partitionNum 分区数量
	 * @return 主题名称
	 * @throws Exception
	 */
	public String getTopic(String topic) throws Exception{
		return getTopic(topic,1,true);
	}
	
	/**
	 * 获取指定的主题
	 * @param topic 主题名称
	 * @param partitionNum 分区数量
	 * @return 主题名称
	 * @throws Exception
	 */
	public String getTopic(String topic,int partitionNum) throws Exception{
		return getTopic(topic,partitionNum,true);
	}
	
	/**
	 * 获取指定的主题
	 * @param topic 主题名称
	 * @param partitionNum 分区数量
	 * @param requireCreate 如果没有主题是否需要主题
	 * @return 主题名称
	 * @throws Exception
	 */
	public String getTopic(String topic,int partitionNum,boolean requireCreate) throws Exception{
		if(containsTopic(topic)) return topic;
		if(!requireCreate) return null;
		
		Integer nodeNum=getClusterNodeNum();
		if(null==nodeNum || 2>nodeNum.intValue()) {
			createTopic(topic);
		}else{
			createTopic(topic,partitionNum,nodeNum-1);
		}
		
		return topic;
	}
	
	/**
	 * 获取Kafka服务端所有主题
	 */
	public Set<String> getAllTopics() throws Exception{
		return adminClient.listTopics().names().get();
	}
	
	/**
	 * 是否包含指定的主题名称
	 * @param topic 主题名称
	 * @return 是否包含
	 * @throws Exception
	 */
	public boolean containsTopic(String topic) throws Exception{
		return getAllTopics().contains(topic);
	}
	
	/**
	 * 是否包含指定的主题集
	 * @param topics 主题名称集
	 * @return 是否包含
	 * @throws Exception
	 */
	public boolean containsAllTopics(Set<String> topics) throws Exception{
		return getAllTopics().containsAll(topics);
	}
	
	/**
	 * 获取Kafka集群服务物理节点数
	 */
	public Integer getClusterNodeNum() throws Exception{
		DescribeClusterResult result=adminClient.describeCluster();
		if(null==result) return null;
		
		Collection<Node> nodeCols=result.nodes().get();
		if(null==nodeCols) return null;
		
		Iterator<Node> nodes=nodeCols.iterator();
		int counter=0;
		
		for(;nodes.hasNext();nodes.next(),counter++);
		return counter;
	}
}
