
2026/11/17 Version: 5.2.0
===
 1. Automatically adapt `clickhouse_settings` to the ClickHouse server version: `input_format_json_throw_on_bad_escape_sequence` (introduced in server v24.4) is now only sent when the connected server supports it. Older servers reject the whole INSERT with an `Unknown setting` error, which blocked commits entirely (verified against a 22.2 server).
 2. The server version is probed once per client with `SELECT version()` — kicked off in the background right after `init()`, cached for the process lifetime and shared by concurrent INSERTs. When the probe fails (e.g. ClickHouse is down) nothing is cached, the setting is omitted for the current round (accepted by every server version), and the probe is retried on the next commit round.
 3. Unit tests for the version comparison logic (`supportsBadEscapeSetting`).

2026/11/16 Version: 5.1.0
===
 1. Codebase rewritten in TypeScript with `strict` compilation. Sources moved from `lib/` to `src/`; `npm run build` compiles them back into `lib/`, so the package entry `lib/index.js` is unchanged. `lib/` is now untracked build output, and all test scripts build before running.
 2. Ship TypeScript type declarations (`types: lib/index.d.ts`) for library consumers.
 3. Fix: pass `role` instead of `roles` to `@clickhouse/client`, so role configuration actually takes effect.
 4. npm publish: only compiled `lib/` is shipped via the `files` whitelist, and `prepublishOnly` forces a fresh build before packing.
 5. Remove unused `@apla/clickhouse` devDependency.
 6. README restructured into separate English/Chinese halves; fixed typos and broken sample code. Note: the config option `cargoPath` documented in previous README is actually `pathToCargoFolder`.

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





