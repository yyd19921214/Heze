# Heze
-----------------------
**简单而高效的Java消息中间件**

## 简介

Heze是一款高性能的Java消息中间件，在总体架构上模仿Kafka实现，但同时也汲取了其他开源项目的可取之处，目前已经完成初步功能，欢迎大家提出宝贵意见。

## 功能特点
- 文件系统利用Index与File结合的方式实现，快速可靠
- 利用ZooKeeper进行集群管理，支持Broker的自动注册与发现
- 网络通信使用Netty模块实现，简单可靠
- Broker支持集群模式与主从复制，提高可靠性
- 支持消费者分组，实现消费者负载均衡
- 支持消息点对点推送和消息订阅两种模式
- 支持Producer的异步与同步两种推送方式

## 模块架构
- client: 负责具体与网络的交互
- consumer/producer: 消费者与生产者
- broker: 负责接收请求，具体处理逻辑由Handle完成
- store: 负责消息的存储


## 最近更新

项目目前正在快速开发中，下一步打算完善Consumer类的设计，加入消费者组概念


## 使用

### server端的启动
```text
public static void main(String[] args) {
    BasicServer basicServer=new BasicServer();
    basicServer.startup(configPath);
    basicServer.registerHandler(RequestHandler.FETCH,new        FetchRequestHandler());
    basicServer.registerHandler(RequestHandler.PRODUCER,new     ProducerRequestHandler());
    try {
        basicServer.waitForClose();
    } catch (InterruptedException e) {
        e.printStackTrace();
    }
}
```

### Producer
```text
BasicProducer producer = BasicProducer.getInstance();
producer.init(producerConfFile);
List<Topic> topics = new ArrayList<>();
for (int i = 1; i <= 5; i++) {
    Topic topic = new Topic();
    topic.setTopic(topicName);
    topic.setContent(String.format(topicContent, i));
    topics.add(topic);
}
Map<String, String> params = new HashMap<>();
params.put("broker", "MyServer01");
producer.send(topics, params);
```

### Consumer
``` text
ServerConfig config = new ServerConfig(configPath);
BasicConsumer consumer=new BasicConsumer(config,topicName);
List<Topic> list = consumer.poll(recordNum);
```
更多功能请参见文档

## 文档

完整的文档可以在[这里]

## TODO

- [ ] 更好的Consumer实现
- [ ] 实现Server端Master/Slave的复制
- [x] 支持异步与同步发送

## 协助开发

1. Fork
2. 从 dev 分支新建一个分支
3. 编写代码，如果可能的话加上测试
4. PR 到原 dev 分支

## LICENSE
MIT

## 关于
作者：杨煜冬（547428014@qq.com）
