# clickhouse-cargo
Clickhouse-cargo accumulates insert queries and commit them to [clickhouse](https://clickhouse.yandex/) in batch jobs with retries on failure.

[Clickhouse is designed for batch data insertion with significant intervals](https://clickhouse.tech/docs/en/introduction/performance/#performance-when-inserting-data).
When inserting data to clickhouse from distributed node clusters, [some sort of centralized queue mechanism is required](https://github.com/ClickHouse/ClickHouse/issues/1067#issuecomment-320471793) in order to prevent Clickhouse from hitting the performance limit.
That brings complexity to the service chain and more difficulty to maintenance.

Clickhouse-cargo provides an easy way to batch data insertion to Clickhouse. It accumulates insert queries to local file caches, and commit them to Clickhouse in batch. Clickhouse-cargo will also automatically restore uncommitted local caches. That efficiently prevents data lose from unexpected Clickhouse accidents.


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


### New Cargo Instanse

```javascript
/*
@param statement String, sql insert statement
@param bulkTTL Int, ttl(in ms) for flush accumlated inserts. default: 5000, min: 1000
*/
const cargo = clickhouse-cargo.createCargo(statement, bulkTTL);
```

### Insert data
```javascript
cargo.push(column0, column1, column2, column3...)
```




