
2021/6/24 Version: 3.1.1
===
 1. Reduce unnecessary log.

2021/6/24 Version: 3.1.0
===
 1. Cargo: now submits multiple(up to config.maxInsetParts) local uncommits in one http post request. This change solves "Too many parts" error on clickhouse server side.
    Note: use multistream to avoid memory leak when hundred of readable stream piping to the upload request.

2021/4/12 Version: 3.0.5
===
 1. Cargo: no longer extends EventEmitter

2021/4/12 Version: 3.0.3
===
 1. Introduce config.maxInsetParts a) to avoid local commits blocked due to a single failure in the commit queue. and b) to avoide `Too many parts` errors happend on clickhouse server-side

2021/3/15 Version: 3.0.2
===
 1. When multiple cargos are running on the same node, there is a large chance that only one cargo will be committed in each exame routine. Remove `Cargo::_isCommiting` lock since no need for that in the current sequential implementqation.
 2. Add handling to pm2 stop event. Refer: https://pm2.keymetrics.io/docs/usage/signals-clean-restart/





