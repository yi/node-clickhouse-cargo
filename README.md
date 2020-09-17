# clickhouse-cargo
Clickhouse-cargo is an utility module which accumulates insert queries and commit them to [clickhouse](https://clickhouse.yandex/) in batch jobs with retries on failure.

[Clickhouse is designed for batch data insertion with significant intervals](https://clickhouse.tech/docs/en/introduction/performance/#performance-when-inserting-data).
When inserting data to clickhouse from distributed node clusters, [some sort of centralized queue mechanism is required](https://github.com/ClickHouse/ClickHouse/issues/1067#issuecomment-320471793) in order to prevent Clickhouse from hitting the performance limit.
That brings complexity to the service chain and more difficulty for maintenance.
Clickhouse-cargo brings an easy way to batch data insertion to Clickhouse. It accumulates insert queries into local file caches, and commit them to Clickhouse in batch. Clickhouse-cargo will also automatically restore uncommitted local caches. That efficiently prevent data lose from unexpected Clickhouse accidents.


## Install
```
$ npm install clickhouse-cargo
```

## Usage





