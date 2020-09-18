# clickhouse-cargo
clickhouse-cargo accumulates insert queries and commit them to [clickhouse](https://clickhouse.yandex/) in batch jobs with retries on failure.

[Clickhouse is designed for batch data insertion with significant intervals](https://clickhouse.tech/docs/en/introduction/performance/#performance-when-inserting-data).
When inserting data to clickhouse from distributed node clusters, [some sort of centralized queue mechanism is required](https://github.com/ClickHouse/ClickHouse/issues/1067#issuecomment-320471793) in order to prevent Clickhouse from hitting the performance limit.
That brings complexity to the service chain and more difficulty to maintenance.

Clickhouse-cargo provides an easy way to batch data insertion to Clickhouse. It accumulates insert queries to local file caches, and commit them to Clickhouse in batch. Clickhouse-cargo will also automatically restore uncommitted local caches. That efficiently prevents data lose from unexpected Clickhouse accidents.

clickhouse-cargo 适用于分布式的 NodeJS 服务向 Clickhouse 频繁插入数据的应用场景。这个模块将向 Clickhouse 数据库的高频写入改为低频的批量插入。

## How it works

 1. The `cargo` instance accepts insert requests submitted by the `push` method and routes these requests to a `bulk`.
 2. The `bulk` writes accumulated `push` in the memory to a local file cache according to the setting of `stream.cork`.
 3. `cargo` checks all online `bulks` regularly. When a `bulk` exceeds its `bulkTTL`,  it will then commit its local file cache to the Clickhouse server.
 4. In case of a Clickhouse commit failure, `bulk` will retry the submission in the next round of inspection cycle until the submission is successful.
 5. In case of the NodeJS process crash. local `bulk` file caches will remain on disk. Thus next time when `clickHouse-cargo` module starts, `cargo` checks the remaining `bulk` cache files, and submit them to Clickhouse again.

## 工作原理

 1. `cargo` 实例接受 `push`方法所提交的插入请求，并将请求路由给 `bulk`。
 2. `bulk` 根据 `stream.cork` 的设定，按量将内存中累计的 `push` 写入本地文件缓存。
 3. `cargo` 定时检查所有在线的 `bulk`, 当 `bulk` 的存活超过 `bulkTTL` 的设定时，将 `bulk` 所对应的本地文件缓存提交到 Clickhouse 服务器。
 4. 当 Clickhouse 写入失败时，`bulk` 将会在下一轮检查周期中重试提交直到提交成功。
 5. 当本地的 NodeJS 进程奔溃时，都会导致本地的 `bulk` 文件缓存残留。于是下一次启动 `clickHouse-cargo` 模块时, `cargo` 检查到残留的 `bulk` 缓存文件时将再次提交给 Clickhouse。

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
const STATEMENT_INSERT = `INSERT INTO cargo_test.table_test`;
const NUM_OF_INSERTIONS = 27891; // NOTE: bulk flushs every 100 lines

// init clickhouse-cargo
clickhouse-cargo.init({
  "host":"play-api.clickhouse.tech",
  "port":"8443",
  "user":"playground",
  "password":"clickhouse"
});

// insert data
const theCargo = clickhouse-cargo.createCargo(STATEMENT_INSERT);
for (let i =0, i < NUM_OF_INSERTIONS, i++){
  theCargo.push(new Date(), i, "string");
}
```

## API

### Initialization

```javascript
clickhouse-cargo.init(options: Options)
```

*Options*

|                  | required | default       | description
| :--------------- | :------: | :------------ | :----------
| `host`           | ✓        |               | Host to connect.
| `cargoPath`      |          | `${cwd()}/cargo_files`              | Path to local cargo cache
| `user`           |          |               | Authentication user.
| `password`       |          |               | Authentication password.
| `path` (`pathname`) |       | `/`           | Pathname of ClickHouse server.
| `port`           |          | `8123`        | Server port number.
| `protocol`       |          | `'http:'`     | `'https:'` or `'http:'`.
| `dataObjects` <br /> `format` <br />`queryOptions` <br /> `timeout`, <br /> `headers`, <br /> `agent`, <br /> `localAddress`, <br /> `servername`, <br /> etc… |   |   |  Any [@apla/node-clickhouse](https://github.com/apla/node-clickhouse#new-clickhouseoptions-options) options are also available.


### Create a Cargo instanse

```javascript
/*
@param statement String, sql insert statement
@param bulkTTL Int, ttl(in ms) for flush accumlated inserts. default: 5000, min: 1000
*/
const cargo = clickhouse-cargo.createCargo(statement, bulkTTL);
```

### Insert a row
```javascript
/*
Instead of inserting to Clickhouse directly, push row to the cargo. And the cargo will commit accumulated insertions to Clickhouse in batch.
*/
cargo.push(column0, column1, column2, column3...)
```




