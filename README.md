# GraalCDC

> 一个基于 Debezium + GraalVM + Graal.js 的轻量级 CDC（Change Data Capture）实验项目。

## 🧪 当前状态

- ✅ **开发中（WIP）**
- 支持从数据库（如 MySQL）捕获变更事件
- 支持通过 JavaScript 脚本自定义处理逻辑
- 内置 Elasticsearch 和 JDBC（关系型数据库）写入能力
- 使用 Spring WebFlux + WebClient 实现异步非阻塞 I/O

> ⚠️ 尚未生产就绪，API 和配置可能随时调整。

## 🛠️ 技术栈

- **变更捕获**：Debezium Embedded Engine
- **脚本引擎**：GraalVM + Graal.js（支持 JS 动态脚本）
- **运行时**：Spring Boot + Spring WebFlux
- **目标存储**：Elasticsearch、MySQL/PostgreSQL（通过 JDBC）
- **部署**：支持 JVM 模式，未来计划支持 GraalVM Native Image

## 🎯 目标

构建一个：
- 资源占用低
- 启动速度快
- 逻辑可脚本化
- 易于嵌入或独立运行

的轻量级 CDC 引擎，适用于边缘同步、开发测试、小型数据管道等场景。

## 📂 项目结构（待完善）


## 🚧 下一步计划


## 🤝 欢迎参与

目前是个人实验项目，但如果你对轻量 CDC、GraalVM 或 Debezium 感兴趣，欢迎提 Issue 或 PR！
