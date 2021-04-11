package com.df.plugin.sink.amqp.util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AbstractDeclarable;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.BindingBuilder.DestinationConfigurer;
import org.springframework.amqp.core.CustomExchange;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.core.ParameterizedTypeReference;

import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description AMQP操作工具
 */
public class AmqpUtil {
	/**
	 * RMQ例程虚拟主机名
	 */
	private String virHost;
	
	/**
	 * RMQ例程登录密码
	 */
	private String passWord;
	
	/**
	 * RMQ例程登录用户名
	 */
	private String userName;
	
	/**
	 * AMQP协议客户端
	 */
	private AmqpAdmin amqpAdmin;
	
	/**
	 * RMQ例程地址列表
	 */
	private ArrayList<String> hostList;
	
	/**
	 * AMQP协议操作工具集
	 */
	private AmqpTemplate amqpTemplate;
	
	/**
	 * 默认ACK回调接口实现
	 */
	private DefaultConfirmCallback confirmCallback;
	
	/**
	 * RMQ连接工厂实现
	 */
	private CachingConnectionFactory connectionFactory;
	
	/**
	 * 永不超时(持续阻塞200年)
	 */
	private static final long TIMEOUT_NEVER=200*365*86400*1000L;
	
	/**
	 * Amqp常用组件映射器
	 */
	private static HashMap<String,Class<? extends AbstractDeclarable>> componentMapper;
	
	static{
		componentMapper=new HashMap<String,Class<? extends AbstractDeclarable>>();
		componentMapper.put("fanout", FanoutExchange.class);
		componentMapper.put("direct", DirectExchange.class);
		componentMapper.put("topic", TopicExchange.class);
		componentMapper.put("queue", Queue.class);
	}
	
	/**
	 * @author Lixiang
	 * @description 抽象自定义交换器
	 */
	public static abstract class AbstractCustomExchange extends CustomExchange {
		public AbstractCustomExchange(String name) {
			super(name, "custom");
		}
		
		public AbstractCustomExchange(String name,String type) {
			super(name, type);
		}
	}
	
	/**
	 * 添加自定义交换器
	 * @param exchangeType 交换器类型
	 * @param typeNames 交换器类型名称
	 * @description 自定义的交换器类必须带有一个字串参数的构造方法
	 */
	public static void addCustomExchange(Class<? extends AbstractCustomExchange> exchangeType,String... typeNames) {
		String typeName=(null==typeNames||0==typeNames.length)?null:null==typeNames[0]?null:typeNames[0];
		if(null==typeName) {
			componentMapper.put(exchangeType.getSimpleName(), exchangeType);
		}else{
			componentMapper.put(typeName, exchangeType);
		}
	}
	
	public AmqpUtil() {
		this.virHost="/";
		this.hostList=new ArrayList<String>();
	}
	
	public AmqpUtil(String hostList) {
		this();
		appendAddress(hostList);
	}
	
	public AmqpUtil(String hostList,String virHost) {
		this();
		setVirHost(virHost);
		appendAddress(hostList);
	}
	
	public AmqpUtil(String hostList,String virHost,String userName,String passWord) {
		this();
		setVirHost(virHost);
		appendAddress(hostList);
		setPassWord(passWord);
		setUserName(userName);
	}
	
	/**
	 * 设置虚拟机名称
	 * @param virHost 虚拟机名称
	 * @description 只能在创建连接工厂之前调用
	 * @return AmqpUtil实例
	 */
	public AmqpUtil setVirHost(String virHost) {
		if(null==virHost) return this;
		if((virHost=virHost.trim()).isEmpty()) return this;
		this.virHost=virHost;
		return this;
	}
	
	/**
	 * 设置密码
	 * @param passWord 密码
	 * @description 只能在创建连接工厂之前调用
	 * @return AmqpUtil实例
	 */
	public AmqpUtil setPassWord(String passWord) {
		if(null==passWord) return this;
		if((passWord=passWord.trim()).isEmpty()) return this;
		this.passWord=passWord;
		return this;
	}
	
	/**
	 * 设置用户名
	 * @param userName 用户名
	 * @description 只能在创建连接工厂之前调用
	 * @return AmqpUtil实例
	 */
	public AmqpUtil setUserName(String userName) {
		if(null==userName) return this;
		if((userName=userName.trim()).isEmpty()) return this;
		this.userName=userName;
		return this;
	}
	
	/**
	 * 追加连接地址
	 * @param hostAndPort 连接地址(host:port)
	 * @description 只能在创建连接工厂之前调用
	 * @return AmqpUtil实例
	 */
	public AmqpUtil appendAddress(String hostAndPort) {
		if(null==hostAndPort) return this;
		if((hostAndPort=hostAndPort.trim()).isEmpty()) return this;
		this.hostList.add(hostAndPort);
		return this;
	}
	
	/**
	 * 设置是否支持发布后获取ACK确认消息
	 * @param publisherConfirms 发布后是否ACK确认
	 * @return AmqpUtil实例
	 */
	public AmqpUtil setPublisherConfirms(boolean publisherConfirms) {
		return setConnectionParameter("publisherConfirms",publisherConfirms);
	}
	
	/**
	 * 设置连接超时时间
	 * @param connectionTimeout 连接超时时间
	 * @return AmqpUtil实例
	 */
	public AmqpUtil setConnectionTimeout(int connectionTimeout) {
		return setConnectionParameter("connectionTimeout",connectionTimeout);
	}
	
	/**
	 * 设置RMQ连接参数
	 * @param name 参数名
	 * @param value 参数值
	 * @return AmqpUtil实例
	 */
	public AmqpUtil setConnectionParameter(String name,Object value) {
		CachingConnectionFactory connectionFactory=getConnectionFactory();
		String methodName=CommonUtil.getSetMethodNameFromFieldName(name);
		Method method=CommonUtil.findMethod(connectionFactory, methodName, value);
		if(null==method) throw new RuntimeException("Not Found "+methodName+" method...");
		try {
			method.invoke(connectionFactory, value);
			return this;
		} catch (Exception e) {
			throw new RuntimeException("Call Method Occur Error...",e);
		}
	}
	
	/**
	 * 设置RMQ模板参数
	 * @param name 参数名
	 * @param value 参数值
	 * @return AmqpUtil实例
	 */
	public AmqpUtil setTemplateParameter(String name,Object value) {
		AmqpTemplate amqpTemplate=getAmqpTemplate();
		String methodName=CommonUtil.getSetMethodNameFromFieldName(name);
		Method method=CommonUtil.findMethod(amqpTemplate, methodName, value);
		if(null==method) throw new RuntimeException("Not Found "+methodName+" method...");
		try {
			method.invoke(amqpTemplate, value);
			return this;
		} catch (Exception e) {
			throw new RuntimeException("Call Method Occur Error...",e);
		}
	}
	
	/**
	 * 获取RMQ连接工厂
	 * @return RMQ连接工厂
	 */
	public synchronized CachingConnectionFactory getConnectionFactory() {
		if(null!=connectionFactory) return connectionFactory;
		if(hostList.isEmpty()) hostList.add("127.0.0.1:5672");
		
		CachingConnectionFactory connectionFactory=new CachingConnectionFactory();
		connectionFactory.setAddresses(CommonUtil.joinToString(hostList, ","));
		if(null!=virHost) connectionFactory.setVirtualHost(virHost);
		if(null!=userName && null!=passWord) {
			connectionFactory.setUsername(userName);
			connectionFactory.setPassword(passWord);
		}
		
		return this.connectionFactory=connectionFactory;
	}
	
	/**
	 * 设置RMQ连接工厂
	 * @param connectionFactory 连接工厂
	 */
	public synchronized void setConnectionFactory(CachingConnectionFactory connectionFactory) {
		this.connectionFactory=connectionFactory;
	}
	
	/**
	 * 获取RMQ客户端
	 * @return RMQ客户端
	 */
	public synchronized RabbitAdmin getAmqpAdmin() {
		if(null!=amqpAdmin) return (RabbitAdmin)amqpAdmin;
		return (RabbitAdmin)(this.amqpAdmin=new RabbitAdmin(getConnectionFactory()));
	}
	
	/**
	 * 获取RMQ例程操作集
	 * @return RMQ操作模板
	 */
	public synchronized RabbitTemplate getAmqpTemplate() {
		if(null!=amqpTemplate) return (RabbitTemplate)amqpTemplate;
		return (RabbitTemplate)(this.amqpTemplate=getAmqpAdmin().getRabbitTemplate());
	}
	
	/**
	 * 是否启用事务
	 * @param transactional 是否启用事务
	 */
	public void setChannelTransacted(boolean transactional) {
		setTemplateParameter("channelTransacted",transactional);
	}
	
	/**
	 * 设置ACK回调
	 * @param confirmCallback ACK回调
	 */
	public void setConfirmCallback(ConfirmCallback confirmCallback) {
		setPublisherConfirms(true);
		setTemplateParameter("confirmCallback",confirmCallback);
	}
	
	/**
	 * 设置默认ACK回调
	 * @param confirmCallback ACK回调
	 */
	public void setDefaultConfirmCallback() {
		setConfirmCallback(this.confirmCallback=new DefaultConfirmCallback());
	}
	
	/**
	 * 同一个客户端的生产者和消费者是否各自使用独立连接(避免因一方阻塞而影响另一方)
	 * @param usePublisherConnection 是否使用独立连接
	 */
	public void setUsePublisherConnection(boolean usePublisherConnection) {
		setTemplateParameter("usePublisherConnection",usePublisherConnection);
	}
	
	/**
	 * 设置强制响应回调
	 * @param returnCallback 响应回调
	 * @param mandatory 是否强制
	 */
	public void setMandatory(ReturnCallback returnCallback,boolean mandatory) {
		setTemplateParameter("returnCallback",returnCallback);
		setTemplateParameter("mandatory",mandatory);
	}
	
	/**
	 * 删除RMQ例程中的绑定(幂等操作)
	 * @param binding 绑定对象
	 */
	public void delBinding(Binding binding) {
		getAmqpAdmin().removeBinding(binding);
	}
	
	/**
	 * 删除RMQ例程中的队列(幂等操作)
	 * @param queueName 队列名称
	 * @return 是否存在指定队列并且被成功删除
	 */
	public boolean delQueue(String queueName) {
		return getAmqpAdmin().deleteQueue(queueName);
	}
	
	/**
	 * 删除RMQ例程中的交换器(幂等操作)
	 * @param queueName 交换器名称
	 * @return 是否存在指定交换器并且被成功删除
	 */
	public boolean delExchange(String exchangeName) {
		return getAmqpAdmin().deleteExchange(exchangeName);
	}
	
	/**
	 * 删除RMQ例程中没有使用的空队列(幂等操作)
	 * @param queueName 队列名称
	 * @param unused 是否考虑未使用
	 * @param empty 是否考虑空队列
	 */
	public void delQueue(String queueName,boolean unused,boolean empty) {
		getAmqpAdmin().deleteQueue(queueName,unused,empty);
	}
	
	/**
	 * 添加队列到RMQ例程(幂等操作)
	 * @param queueName 队列名称
	 * @return 队列对象
	 */
	public void addQueue(Queue queue) {
		getAmqpAdmin().declareQueue(queue);
	}
	
	/**
	 * 添加交换器到RMQ例程(幂等操作)
	 * @param exchange 交换器
	 */
	public void addExchange(Exchange exchange) {
		getAmqpAdmin().declareExchange(exchange);
	}
	
	/**
	 * 添加延迟交换器(需要AMQP支持)到RMQ例程(幂等操作)
	 * @param exchange 交换器
	 */
	public CustomExchange addDelayExchange(String exchangeName) {
		CustomExchange exchange=getDelayExchange(exchangeName,true,false);
		addExchange(exchange);
		return exchange;
	}
	
	/**
	 * 获取延迟交换器(需要AMQP支持)
	 * @param exchange 交换器
	 * @param isPersist 是否将交换器持久化存储
	 * @param autoDelete 使用后是否自动删除交换器
	 * @return 延迟交换器
	 */
	public CustomExchange getDelayExchange(String exchange,Boolean isPersist,Boolean autoDelete){
		Map<String,Object> args=new HashMap<String,Object>();
		args.put("x-delayed-type", "direct");
		return new CustomExchange(exchange,"x-delayed-message",isPersist,autoDelete,args);
	}
	
	/**
	 * 添加组件(队列或交换器)到RMQ例程(幂等操作)
	 * @param compName 交换器名称(队列或交换器的名称)
	 * @param compType 组件类型(队列或交换器的类型)
	 * @description 本方法支持自定义的交换器类型
	 * @return 队列或交换器对象
	 */
	public <R extends AbstractDeclarable> R addComponent(String compName,Class<R> compType) {
		R comp=null;
		try {
			comp=compType.getConstructor(String.class).newInstance(compName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		if(comp instanceof Queue) {
			getAmqpAdmin().declareQueue((Queue)comp);
		}else if(comp instanceof Exchange){
			getAmqpAdmin().declareExchange((Exchange)comp);
		}
		
		return comp;
	}
	
	/**
	 * 添加路由拓扑绑定
	 * @param destComp 目标组件对象(队列或交换器)
	 * @param srcExchange 源交换器对象(只能是交换器)
	 * @param routingKey 路由键(可为空),自定义名称
	 * @return 绑定对象
	 */
	public Binding addCompBinding(AbstractDeclarable destComp,Exchange srcExchange,String... routingKey){
		Binding binding=null;
		DestinationConfigurer destConfig=(destComp instanceof Queue)?BindingBuilder.bind((Queue)destComp):BindingBuilder.bind((Exchange)destComp);
		
		if(srcExchange instanceof FanoutExchange) {
			binding=destConfig.to((FanoutExchange)srcExchange);
		}else if(srcExchange instanceof TopicExchange){
			binding=destConfig.to((TopicExchange)srcExchange).with(routingKey[0]);
		}else if(srcExchange instanceof DirectExchange){
			binding=destConfig.to((DirectExchange)srcExchange).with(routingKey[0]);
		}else{
			binding=destConfig.to((CustomExchange)srcExchange).with(routingKey[0]).noargs();
		}
		
		getAmqpAdmin().declareBinding(binding);
		return binding;
	}
	
	/**
	 * 添加路由拓扑绑定
	 * @param destName 目标组件名称(队列或交换器),自定义名称
	 * @param destType 目标组件类型(队列或交换器),可选值:queue、exchange
	 * @param srcName 源组件名称(只能是交换器),自定义名称
	 * @param srcType 源组件类型(只能是交换器),可选值:fanout、topic、direct及自定义的名称
	 * @param routingKey 路由键(可为空),自定义名称
	 * @return 绑定对象
	 */
	public Binding addBinding(String destName,String destType,String srcName,String srcType,String... routingKey){
		AbstractDeclarable srcExchange=addComponent(srcName,componentMapper.get(srcType));
		AbstractDeclarable destComponent=addComponent(destName,componentMapper.get(destType));
		DestinationConfigurer destConfig="queue".equals(destType)?BindingBuilder.bind((Queue)destComponent):BindingBuilder.bind((Exchange)destComponent);
		
		Binding binding=null;
		switch(srcType) {
			case "fanout":
				binding=destConfig.to((FanoutExchange)srcExchange);
				break;
			case "topic":
				binding=destConfig.to((TopicExchange)srcExchange).with(routingKey[0]);
				break;
			case "direct":
				binding=destConfig.to((DirectExchange)srcExchange).with(routingKey[0]);
				break;
			default:
				binding=destConfig.to((CustomExchange)srcExchange).with(routingKey[0]).noargs();
		}
		
		getAmqpAdmin().declareBinding(binding);
		return binding;
	}
	
	/**
	 * 异步无阻塞接收消息
	 * @param queue 队列
	 * @description 该方法是无阻塞的,会立即返回(但可能是NULL)
	 * @return 消息对象
	 */
	public Object asyncRecv(String queue){
		return getAmqpTemplate().receiveAndConvert(queue);
	}
	
	/**
	 * 异步无阻塞接收消息
	 * @param queue 队列
	 * @param msgType 消息返回类型
	 * @description 该方法是无阻塞的,会立即返回(但可能是NULL)
	 * @return 消息对象
	 */
	public <R> R asyncRecv(String queue,Class<R> msgType){
		return getAmqpTemplate().receiveAndConvert(queue,ParameterizedTypeReference.forType(msgType));
	}
	
	/**
	 * 同步阻塞接收消息
	 * @param queue 队列
	 * @param maxTimeoutMills 拉取超时时间
	 * @description 该方法在没有消息时会阻塞给定的最大超时阈值,超时后返回NULL
	 * @return 消息对象
	 */
	public Object syncRecv(String queue,long maxTimeoutMills){
		return getAmqpTemplate().receiveAndConvert(queue, maxTimeoutMills);
	}
	
	/**
	 * 同步阻塞接收消息
	 * @param queue 队列
	 * @param msgType 消息返回类型
	 * @description 该方法将一直阻塞直到有消息可用为止
	 * @return 消息对象
	 */
	public Object syncRecv(String queue){
		return getAmqpTemplate().receiveAndConvert(queue,TIMEOUT_NEVER);
	}
	
	/**
	 * 同步阻塞接收消息
	 * @param queue 队列
	 * @param msgType 消息返回类型
	 * @description 该方法将一直阻塞直到有消息可用为止
	 * @return 消息对象
	 */
	public <R> R syncRecv(String queue,Class<R> msgType){
		return getAmqpTemplate().receiveAndConvert(queue,TIMEOUT_NEVER,ParameterizedTypeReference.forType(msgType));
	}
	
	/**
	 * 同步阻塞接收消息
	 * @param queue 队列
	 * @param msgType 消息返回类型
	 * @param maxTimeoutMills 拉取超时时间
	 * @description 该方法在没有消息时会阻塞给定的最大超时阈值,超时后返回NULL
	 * @return 消息对象
	 */
	public <R> R syncRecv(String queue,long maxTimeoutMills,Class<R> msgType){
		return getAmqpTemplate().receiveAndConvert(queue, maxTimeoutMills,ParameterizedTypeReference.forType(msgType));
	}
	
	/**
	 * 发送消息到广播交换器
	 * @param exchange 交换器
	 * @param message 消息对象
	 * @param delayTimes 延迟时间(需要AMQP服务器支持)
	 * @description 交换器的类型声明为FanoutExchange
	 * @return 若不存在ACK回调则返回NULL,否则返回值表示消息是否发布成功
	 */
	public Boolean fanoutSend(String exchange,Object message,Long... delayTimes){
		return commonSend(exchange,"fanout",message,delayTimes);
	}
	
	/**
	 * 发送消息到通用交换器
	 * @param exchange 交换器
	 * @param routingKey 路由键
	 * @param message 消息对象
	 * @param delayTimes 延迟时间(需要AMQP服务器支持)
	 * @description 交换器的类型取决于调用本方法前的声明类型
	 * @return 若不存在ACK回调则返回NULL,否则返回值表示消息是否发布成功
	 */
	public Boolean commonSend(String exchange,String routingKey,Object message,Long... delayTimes) {
		Long delayTime=null==delayTimes||0==delayTimes.length?null:delayTimes[0];
		RabbitTemplate rabbitTemplate=getAmqpTemplate();
		if(null==delayTime) {
			rabbitTemplate.convertAndSend(exchange, routingKey,message);
		}else{
			rabbitTemplate.convertAndSend(exchange,routingKey, message,new DelayMessagePostProcessor(delayTime));
		}
		return null==confirmCallback?null:confirmCallback.success;
	}
	
	/**
	 * ACK回调接口
	 * @author Lixiang
	 */
	private static class DefaultConfirmCallback implements ConfirmCallback{
		/**
		 * 是否发送成功
		 */
		private boolean success=true;

		@Override
		public void confirm(CorrelationData correlationData, boolean ack, String cause) {
			this.success=ack;
			if(null!=cause) System.out.println(cause);
		}
	}
	
	/**
	 * @author Lixiang
	 * @description 延迟消息后处理器
	 * 延迟消息的实现原理如下:
	 * 发送到延迟交换器中的消息会等待消息实体(Message对象)中指定的时间之后再根据给定的
	 * 路由键转发到对应队列中,这样消费者方总是会延迟给定的时间之后才会接收到消息,在等待的
	 * 这一段时间中,被发送的消息实际上是被缓存到延迟交换器中的
	 */
	public static class DelayMessagePostProcessor implements MessagePostProcessor{
		/**
		 * 延迟时间(毫秒)
		 */
		private Long delayTime;
		
		public DelayMessagePostProcessor(){
			this.delayTime=5000L;
		}
		
		public DelayMessagePostProcessor(Long delayTime){
			this.delayTime=delayTime;
		}

		@Override
		public Message postProcessMessage(Message message) throws AmqpException {
			message.getMessageProperties().setHeader("x-delay", delayTime);
			return message;
		}
	}
}