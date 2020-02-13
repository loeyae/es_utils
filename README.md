# es_utils
基于ElasticSearch Java High Level Rest Client的简易封装，适用springboot项目。

## es配置
```$yaml
elasticsearch:
  cluster-nodes: ${ES_SERVER_HOST}
```
多个节点以逗号分隔，比如
```$yaml
elasticsearch:
  cluster-nodes: 192.168.0.2:9200,192.168.0.2:9300
```

使用方法可参考单元测试。