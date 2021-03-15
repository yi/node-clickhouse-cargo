
2021/3/15 Version: 3.0.2
===
A bug was found in `index:: examCargos` routine.
When multiple cargos are running on the same node, there is a large chance that only one cargo will be examed in each routine.
I suspect that is caused by incorrectly use of for loop with await.
Thus replace that loop by the npm async module.





