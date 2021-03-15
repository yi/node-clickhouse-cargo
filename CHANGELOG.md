
2021/3/15 Version: 3.0.2
===
 1. When multiple cargos are running on the same node, there is a large chance that only one cargo will be committed in each exame routine. Remove `Cargo::_isCommiting` lock since no need for that in the current sequential implementqation.
 2. Add handling to pm2 stop event. Refer: https://pm2.keymetrics.io/docs/usage/signals-clean-restart/





