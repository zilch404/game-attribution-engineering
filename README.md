# Big Data Roadmap

一个面向实践的大数据与数据工程学习仓库。目标是从基础知识出发，逐步完成批处理、流处理、数据仓库与云原生数据平台项目。

## 学习路线

### 1. 基础能力

- [ ] Linux、Shell 与 Git
- [ ] Python 基础、虚拟环境与包管理
- [ ] SQL：查询、聚合、窗口函数与执行计划
- [ ] 数据结构、算法与计算机网络基础
- [ ] Docker 基础

### 2. 数据存储

- [ ] 关系型数据库：PostgreSQL / MySQL
- [ ] NoSQL：Redis、MongoDB、Cassandra
- [ ] 列式存储与文件格式：Parquet、ORC、Avro
- [ ] 对象存储：S3 兼容存储
- [ ] 分区、索引、压缩与数据生命周期

### 3. 分布式系统

- [ ] CAP、BASE 与一致性模型
- [ ] 分区、复制、容错与共识
- [ ] Hadoop、HDFS 与 MapReduce
- [ ] 资源调度：YARN / Kubernetes
- [ ] 可观测性与故障排查

### 4. 批处理

- [ ] Apache Spark 基础
- [ ] Spark SQL 与 DataFrame
- [ ] Shuffle、分区与性能优化
- [ ] 数据倾斜处理
- [ ] 使用 Airflow 编排批处理任务

### 5. 流处理

- [ ] Apache Kafka：主题、分区、消费组
- [ ] Kafka Connect 与 Schema Registry
- [ ] Apache Flink / Spark Structured Streaming
- [ ] Event Time、Watermark 与窗口
- [ ] Exactly-once 语义与状态管理

### 6. 数据仓库与湖仓

- [ ] 维度建模：事实表、维度表与 SCD
- [ ] ETL / ELT 与数据质量
- [ ] dbt 基础
- [ ] Hive Metastore
- [ ] Iceberg / Delta Lake / Hudi
- [ ] Trino / Presto 查询引擎

### 7. 工程化与治理

- [ ] 单元测试、集成测试与数据测试
- [ ] CI/CD 与基础设施即代码
- [ ] 数据目录、血缘与元数据
- [ ] 权限控制、隐私与合规
- [ ] 成本、性能和容量规划

## 实战项目

| 阶段 | 项目 | 目标 |
|---|---|---|
| 入门 | 日志分析流水线 | 使用 Python、SQL 和 Parquet 完成清洗与聚合 |
| 进阶 | 批处理数据仓库 | 使用 Spark、Airflow 和 dbt 构建每日指标 |
| 进阶 | 实时事件分析 | 使用 Kafka 和 Flink 计算实时指标 |
| 综合 | Lakehouse 平台 | 使用对象存储、Iceberg 和 Trino 搭建湖仓 |
| 综合 | 数据质量与观测 | 为流水线增加测试、告警、血缘和仪表盘 |

## 推荐目录

```text
.
├── notes/          # 学习笔记
├── exercises/      # 小练习
├── projects/       # 实战项目
├── datasets/       # 小型示例数据或下载说明
├── docker/         # 本地实验环境
└── resources/      # 书籍、课程与文档索引
```

## 学习方式

1. 每个主题先记录核心概念，再完成一个可运行的小实验。
2. 项目必须包含 README、架构说明、运行步骤和验证结果。
3. 优先使用小数据集验证正确性，再逐步测试扩展性。
4. 每周复盘已完成内容、遇到的问题和下一步计划。

## 进度记录

建议在 Issues 中为每个学习主题创建任务，并使用 Milestones 管理阶段目标。

## License

This project is licensed under the Apache License 2.0.
