# clickhouse-cargo

**English** | [中文文档](#中文文档)

clickhouse-cargo accumulates insert queries and commits them to [ClickHouse](https://clickhouse.yandex/) in batch jobs with retries on failure.

[ClickHouse is designed for batch data insertion with significant intervals](https://clickhouse.tech/docs/en/introduction/performance/#performance-when-inserting-data).
When inserting data to ClickHouse from distributed node clusters, [some sort of centralized queue mechanism is required](https://github.com/ClickHouse/ClickHouse/issues/1067#issuecomment-320471793) in order to prevent ClickHouse from hitting the performance limit.
That brings complexity to the service chain and more difficulty to maintenance.

Clickhouse-cargo provides an easy way to batch data insertion to ClickHouse. It accumulates insert queries to local file caches, and commits them to ClickHouse in batch. Clickhouse-cargo will also automatically restore uncommitted local caches. That efficiently prevents data loss from unexpected ClickHouse accidents.

## How it works

 1. A `cargo` instance accepts insert requests submitted by the `push` method and keeps inserts in-memory.
 2. The `cargo` instance periodically flushes in-memory inserts to a file cache, then rotates this file and commits rotations to the ClickHouse server.
 3. In case of a ClickHouse commit failure, the cargo will retry the submission in the next round of its routine until the submission is successful.
 4. In case of a NodeJS process crash, in-memory inserts will be flushed immediately into the file cache.

### Cluster mode support [(Demo Code)](src/cluster-test/05-restore-bulks-when-cluster.ts)

When running in cluster mode (such as [PM2 cluster deployment](https://pm2.keymetrics.io/docs/usage/cluster-mode/)), all cargo workers will run through an election via UDP communication @ 127.0.0.1:17888 to elect a leader worker. Then only the leader worker will carry on with file rotations and commits.

## NodeJS version requirements

[NodeJS Version: 18.x and above](https://clickhouse.com/docs/en/integrations/javascript#environment-requirements-nodejs)

## Install

```
$ npm install clickhouse-cargo
```

## Usage

```javascript
/*
sample table schema
`CREATE TABLE IF NOT EXISTS cargo_test.table_test
(
  \`time\` DateTime ,
  \`step\`  UInt32,
  \`pos_id\` String DEFAULT ''
)
ENGINE = Memory()`
*/

const clickhouseCargo = require("clickhouse-cargo");
const TABLE_NAME = `default.table_test`;
const NUM_OF_INSERTIONS = 27891; // NOTE: bulk flushes every 100 lines

// init clickhouse-cargo
clickhouseCargo.init({
  "url": "https://play-api.clickhouse.tech:8443",
  "username": "playground",
  "password": "clickhouse"
});

// insert data
const theCargo = clickhouseCargo.createCargo(TABLE_NAME);
for (let i = 0; i < NUM_OF_INSERTIONS; i++) {
  theCargo.push(new Date(), i, "string");
}
```

### Usage examples

This cargo module is designed for inserting large numbers of records in a few batches. Thus it will be helpful to have some sort of insertion generator/validation.

I'd used [Joi](https://joi.dev/api/) for a while, and found it consumed too much CPU power, and [here is an example of how we are currently using the cargo module](https://github.com/yi/node-clickhouse-cargo/issues/1#issuecomment-1005407802)

## API

### Initialization

__Init by code__

```javascript
clickhouseCargo.init(options: Options)
```

*Options*

|                  | required | default       | description
| :--------------- | :------: | :------------ | :----------
| @clickhouse/client Configurations             |          |               |  https://clickhouse.com/docs/en/integrations/javascript#configuration
| `pathToCargoFolder` |       | `${cwd()}/cargo_files` | Path to local cargo cache.
| `maxTime`        |          |  1000         | For how long in milliseconds, a cargo will keep in-memory insert buffer before flushing it to file.
| `maxRows`        |          |  100          | For how many rows a cargo will keep in-memory.
| `commitInterval` |          |  5000         | Interval(ms) for cargo to commit to ClickHouse.
| `maxInsetParts`  |          |  100          | For how many parts will be inserted into ClickHouse in a single exam routine. Keep the value less than 300 to avoid the `Too many parts` issue on the ClickHouse server-side.
| `saveWhenCrash`  |          |  true         | When `false`, cargos will not flushSync in-memory data when the node process crashes.

__Init by the environment variable__

Init by the environment variable is recommended for real-world production.
Clickhouse-cargo recognises `process.env.CLICKHOUSE_CARGO_PROFILE` and seeks the config JSON file from `~/.clickhouse-cargo/${process.env.CLICKHOUSE_CARGO_PROFILE}.json`

### Create a Cargo instance

```javascript
/*
* @param tableName String, the name of the ClickHouse table into which data is inserted
*/
const cargo = clickhouseCargo.createCargo(tableName);
```

### Insert a row

```javascript
/*
Instead of inserting to ClickHouse directly, push rows to the cargo. And the cargo will commit accumulated insertions to ClickHouse in batch.
*/
cargo.push(column0, column1, column2, column3...)
```

---

# 中文文档

clickhouse-cargo 适用于分布式的 NodeJS 服务向 ClickHouse 频繁插入数据的应用场景。这个模块将向 ClickHouse 数据库的高频写入改为低频的批量插入。

## 工作原理

 1. `cargo` 实例接受 `push` 方法所提交的插入请求，并将请求临时存放于内存中。
 2. `cargo` 周期性地将内存中累积的插入记录写入对应的文件缓存。随后将文件缓存进行滚动，并将滚出结果提交到 ClickHouse 数据库。
 3. 当向 ClickHouse 写入失败时，`cargo` 将会在下一轮检查周期中重试提交，直到提交成功。
 4. 当本地的 NodeJS 进程崩溃时，内存中累积的插入请求会被同步写入对应的文件缓存。

### 支持集群模式 [(示例代码)](src/cluster-test/05-restore-bulks-when-cluster.ts)

在集群模式下（例如 [PM2 集群部署](https://pm2.keymetrics.io/docs/usage/cluster-mode/)），所有的 cargo worker 将通过 UDP 通讯（127.0.0.1:17888）选举出一个领头的 worker。接着由这个领头的 worker 来负责文件缓存的滚动和提交到 ClickHouse 数据库。

## NodeJS 版本要求

[NodeJS 18.x 及以上](https://clickhouse.com/docs/en/integrations/javascript#environment-requirements-nodejs)

## 安装

```
$ npm install clickhouse-cargo
```

## 使用

```javascript
/*
示例表结构
`CREATE TABLE IF NOT EXISTS cargo_test.table_test
(
  \`time\` DateTime ,
  \`step\`  UInt32,
  \`pos_id\` String DEFAULT ''
)
ENGINE = Memory()`
*/

const clickhouseCargo = require("clickhouse-cargo");
const TABLE_NAME = `default.table_test`;
const NUM_OF_INSERTIONS = 27891; // NOTE: bulk flushes every 100 lines

// init clickhouse-cargo
clickhouseCargo.init({
  "url": "https://play-api.clickhouse.tech:8443",
  "username": "playground",
  "password": "clickhouse"
});

// insert data
const theCargo = clickhouseCargo.createCargo(TABLE_NAME);
for (let i = 0; i < NUM_OF_INSERTIONS; i++) {
  theCargo.push(new Date(), i, "string");
}
```

### 使用示例

此 cargo 模块的设计目标是：以少量批次向 ClickHouse 插入大量记录。因此，搭配某种插入数据生成器 / 校验器会很有帮助。

作者曾使用 [Joi](https://joi.dev/api/) 一段时间，发现其 CPU 消耗过高。[这里是当前使用 cargo 模块的一个例子](https://github.com/yi/node-clickhouse-cargo/issues/1#issuecomment-1005407802)。

## API / 接口

### 初始化

__通过代码初始化__

```javascript
clickhouseCargo.init(options: Options)
```

*Options 参数*

|                  | 必填 | 默认值       | 说明
| :--------------- | :------: | :------------ | :----------
| @clickhouse/client 配置项             |          |               |  https://clickhouse.com/docs/en/integrations/javascript#configuration
| `pathToCargoFolder` |       | `${cwd()}/cargo_files` | 本地 cargo 缓存目录
| `maxTime`        |          |  1000         | cargo 在内存中缓存插入记录的最长时间（毫秒），超过后将写入文件
| `maxRows`        |          |  100          | cargo 在内存中缓存的行数上限
| `commitInterval` |          |  5000         | cargo 向 ClickHouse 提交的间隔时间（毫秒）
| `maxInsetParts`  |          |  100          | 单次检查周期内向 ClickHouse 插入的最大 parts 数量。请保持该值小于 300，以避免 ClickHouse 服务端出现 `Too many parts` 问题
| `saveWhenCrash`  |          |  true         | 为 `false` 时，node 进程崩溃时 cargo 将不会把内存中的数据同步写入文件

__通过环境变量初始化__

推荐在生产环境中通过环境变量初始化。
Clickhouse-cargo 识别 `process.env.CLICKHOUSE_CARGO_PROFILE`，并从 `~/.clickhouse-cargo/${process.env.CLICKHOUSE_CARGO_PROFILE}.json` 读取配置文件

### 创建一个 Cargo 实例

```javascript
/*
* @param tableName 字符串，数据所要插入的 ClickHouse 表名
*/
const cargo = clickhouseCargo.createCargo(tableName);
```

### 插入一行

```javascript
/*
无需直接向 ClickHouse 插入，将数据 push 给 cargo 即可。cargo 会将累积的插入批量提交到 ClickHouse。
*/
cargo.push(column0, column1, column2, column3...)
```
