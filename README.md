# 测试数据对比工具

## 介绍
```
两个IoTDB数据对比工具，可用于版本升级、数据迁移等过程
提供两个接口，分别为：对比数据导出和对比，其中导出接口用于升级/迁移前数据库无法再次启动场景
对比策略：
1.storage group对比
2.device 对比
3.measurement对比
4.device数据count对比
5.device数据min max time对比
6.device数据近一个月每日count limit100(top,botoom) top100 bottom100
7.device数据近一年每月count limit100(top,botoom) top100 bottom100
8.device数据一年前count limit100(top,botoom) top100 bottom100
```