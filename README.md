# clickhouse-cargo
clickhouse-cargo accumulates insert queries and commit them to [clickhouse](https://clickhouse.yandex/) in batch jobs with retries on failure.

[Clickhouse is designed for batch data insertion with significant intervals](https://clickhouse.tech/docs/en/introduction/performance/#performance-when-inserting-data).
When inserting data to clickhouse from distributed node clusters, [some sort of centralized queue mechanism is required](https://github.com/ClickHouse/ClickHouse/issues/1067#issuecomment-320471793) in order to prevent Clickhouse from hitting the performance limit.
That brings complexity to the service chain and more difficulty to maintenance.

Clickhouse-cargo provides an easy way to batch data insertion to Clickhouse. It accumulates insert queries to local file caches, and commit them to Clickhouse in batch. Clickhouse-cargo will also automatically restore uncommitted local caches. That efficiently prevents data lose from unexpected Clickhouse accidents.

clickhouse-cargo 适用于分布式的 NodeJS 服务向 Clickhouse 频繁插入数据的应用场景。这个模块将向 Clickhouse 数据库的高频写入改为低频的批量插入。

## How it works

 1. A `cargo` instance accepts insert requests submitted by the `push` method and keep inserts in-memory.
 2. The `cargo` instance periodically flushs in-memory inserts to a file cache, then rotates this file and commits rotations to the Clickhouse server.
 3. In case of a Clickhouse commit failure, the cargo will retry the submission in the next round of its routine till the submission is successful.
 4. In case of the NodeJS process crash. in-memory inserts will be flushed immediately into the file cache.

### Cluster mode support

When running in cluster mode (such as [PM2 cluster deployment](https://pm2.keymetrics.io/docs/usage/cluster-mode/) ), all cargo workers will run through an election via udp communication @ 127.0.0.1:17888 to elect a leader worker. Then only the leader worker will carry on with file rotations and commitments.

## 工作原理

 1. `cargo` 实例接受 `push`方法所提交的插入请求，并将请求临时存放于内存中。
 1. `cargo` 周期性地将内存中累积的插入记录写入对应的文件缓存。随后将文件缓存进行滚动，并将滚出结果提交到 Clickhouse 数据库。
 4. 当向 Clickhouse 写入失败时，`cargo` 将会在下一轮检查周期中重试提交直到提交成功。
 5. 当本地的 NodeJS 进程奔溃时，内存中累积的插入请求会被同步写入对应的文件缓存。

### 支持集群模式

在集群模式下，所有的 cargo woker 将通过UDP通讯选举出一个领头的worker。 接着由这个领头的worker来负责文件缓存的滚动和提交到 Clickhouse 数据库。


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
ENGINE = Memory()`;
*/

const clickhouse-cargo = require("clickhouse-cargo");
const TABLE_NAME = `cargo_test.table_test`;
const NUM_OF_INSERTIONS = 27891; // NOTE: bulk flushs every 100 lines

// init clickhouse-cargo
clickhouse-cargo.init({
  "host":"play-api.clickhouse.tech",
  "port":"8443",
  "user":"playground",
  "password":"clickhouse"
});

// insert data
const theCargo = clickhouse-cargo.createCargo(TABLE_NAME);
for (let i =0, i < NUM_OF_INSERTIONS, i++){
  theCargo.push(new Date(), i, "string");
}
```

### Usage examples

This cargo module is designed for inserting large number of records in a few batches. Thus it will helpful to have some sort of insertion generator/validation.

I'd used [Joi](https://joi.dev/api/) for a while, and found it consumed too much cpu power, and [here is an example of how we are currently using the cargo module](https://github.com/yi/node-clickhouse-cargo/issues/1#issuecomment-1005407802)


## API

### Initialization

__Init by code__


```javascript
clickhouse-cargo.init(options: Options)
```

*Options*

|                  | required | default       | description
| :--------------- | :------: | :------------ | :----------
| `host`           | ✓        |               | Host to connect.
| `cargoPath`      |          | `${cwd()}/cargo_files`              | Path to local cargo cache.
| `maxTime`        |          |  1000         | For how long in milliseconds, a cargo will keep in-memory insert buffer before flushing it to file.
| `maxRows`        |          |  100          | For how many rows a cargo will keep in-memory.
| `commitInterval` |          |  5000         | Interval(ms) for cargo to commit to ClickHouse.
| `maxInsetParts`  |          |  100          | For how many parts will be inserted into ClickHouse in a single exame routine. Keep value less then 300 to avoide `Too many parts` issue happend on clickhouse server-side
| `saveWhenCrash`  |          |  true         | When `false`, cargos will not flushSync in-memory data when node process crashes.
| `user`           |          |               | Authentication user.
| `password`       |          |               | Authentication password.
| `port`           |          | `8123`        | Server port number.
| `protocol`       |          | `'http:'`     | `'https:'` or `'http:'`.


__Init by the environment variable__

Init by the environment variable is recommended for real-world production.
Clickhouse-cargo recognises `process.env.CLICKHOUSE_CARGO_PROFILE` and seeks the config json file from `~/.clickhouse-cargo/${process.env.CLICKHOUSE_CARGO_PROFILE}`


### Create a Cargo instanse

```javascript
/*
* @param tableName String, the name of ClickHouse table which data is inserted
*/
const cargo = clickhouse-cargo.createCargo(tableName);
```

### Insert a row
```javascript
/*
Instead of inserting to Clickhouse directly, push row to the cargo. And the cargo will commit accumulated insertions to Clickhouse in batch.
*/
cargo.push(column0, column1, column2, column3...)
```




