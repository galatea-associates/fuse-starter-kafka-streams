# Problem statement
When making a modification to a KafkaStreams service, frequently it is required to do a streams reset, which deletes all current state and re-consumes the input records in order to use the new topology/schemas correctly. Since records were previously output with the old calculations, there is no guarantee that the set of keys output by the service is a proper or superset of the keys previously created. Any keys created previously but not overwritten by the updated service will continue to create state in downstream processes until those processes are also reset. In this way, a reset to a streams service may require many other services to be reset, in order to bring all of them in line with the keys output post-reset.

In this example, keyC is not output after updating the topology, so keyC will have created downstream state that is no longer valid
```
Version 1:
Output: {keyA, keyB, keyC}
Version 2:
Output: {keyA, keyB}
```

# Proposed solution
Create framework that handles state management such that resets are not necessary. When a service is updated, the service will detect that it has been updated and will do the recalculation without requiring a reset, and for any keys that are not overridden by the recalculation - they will be tombstoned so downstream services will remove their impact from their calculations.

# Scenarios to handle:
- new source topic
- remove source topic
- source topic key change
- start tracking additional (intermediate) state for calculation
- change of private schema
- stop tracking some (intermediate) state for calculation
- configuration update to service
- java logic update

## Scenario diagrams
<!-- To edit diagrams, go to https://www.plantuml.com/plantuml/uml/Km00 and copy the svg url into "decode URL" and click "decode"
When finished editing, right click "View as SVG" and select "Copy link URL" and paste in place of current URL -->
### Basic topology (single input, single processor)
![Basic topology](https://www.plantuml.com/plantuml/svg/hP71IWCn48RlUOhnPg4zzBe7AUX5H1GhlPGUnisWePjCI2OBHVhk9cbbkolqv36PR_xvc5b5mI39Tkf6liWIj1HIH-XKt1ldCQ253sptdu6Jq_VSSO1duwGDX5Qf38XfZKSdyEXyadVsfWMCOEHlyJLmIp5og80V0MzKNsBObMgepn7nW8AZ13rEU4cIPwobc9KoHwMCmE8kZshWUq-7vHnwx18jZygn471FeNW0h0M56WLJZ9o6WkOaNv9EMZcjo3pHOGxRouRTKPs8mcf_ZHi4z4ZWDXwYa9t1Xa8qx1heb-NRb0NEhWGEgfVH3PnN1WieDnCwxsbJ-zJ_UrH6A5ZI1eK6D4ENXJgfubo5ChZjC_DBx-Q3wUGMkjw2OpDaJ-5LffFjVW00)

### Recalculation flow without changes
![Basic Recalculation](https://www.plantuml.com/plantuml/svg/xLN1JXin4BtxAuPJ4vGqWYC2X0INggfQbOeU40V7EvkuyThRyJWHGlnxxLr76uE2AAhQIq_HpZo_UUzvh5nubiIXCUA0bzWWAAHWK5hXLzgsacG3SwbM2tB1LfVEE89Fsl8beMo4Y03MIhVICdomRU1lhjKAf0VDpvjVo2dqtb47Q0l0L-o-M3kRkjHz5eWhoR9Wg4g6pu7ZZ6jEobBRUI6iOmItHafWW6kMZ2S2OEJlFMCpXXia7qUUmFeebNMIEOQ7DEiZtXzDu7RL_Qob2NXqzzY36Ad1Ial60hlRw4PrS1Uu9xrTHymMVJV9WcywvaU1je8aLuWP3B9r8VjX45CSANoUn2oLD2eOsQN9f1SB9AoUmhxh5WlDuDEWtqyjb_FS22V6AZQFtsI_Qc5wdXSWUQa9_H88VmRqBFfwGkZsUNUGthLzOeryqfW8KOugBuGk-5-uwc5KrRu8V99hNbAOWYAKZ8NJFnlkFzpN4dcy7xbtJNreMqf1zJpbpGntgrpJ6cGix-Dv-t-oW_VP32wNg5PGEuAesmCl9SC62Q4bN6iNlBdFmSCySA81MJCIK1V7uI5CWLqpz-mi2W0TUv8ndxbcHzZdg6iO7UnKtojdMMhhHwNuSRGJuP0qRIHLeo4k6umPTxrT7i8cEXgLOINOetYJnT1Mah4gRC8qb_SvFCuETuKja9-gWeEpucxsUYrHdPbNkHQGXMwt63QQbtbtSsdlIzlom59Mu6M3_HR377c3Q67Td_SJi2wBUVrp-WilvlNlm63DK3l7I71QEykWd75qDZTIhSxZKbyv-uxXH_0Cnhb-UrGWGilR62P3HngphNemwBibipj_i9X3E9sbOUVY8ae9ZVa5)

### Recalculation with minor logic change
![Minor change Recalculation](https://www.plantuml.com/plantuml/svg/nLLDRnCn4BtxLqnlITLG9STAe4hXWX02jL0EY8FhdIHMlFOo7gTKLVuxziOfpfRjXag8ctVyPlpUVCnUUfR4eJRYX5TO8oYaO51QuTVQDf9a3RTIhPVaWgtcpZY23zgo8fIr4170Mkb6MePtjWdysJLQFJL_8gVGUqS7LrVOVh5szi3-LhByiyV7m37QDUzUjuuHt0Qf-yWrIyOB0R36--a5RAG9E1LeAqWkuXmUFSktIiUHl_TZk47oaLoCC4qsdLuPmvn_lc_ZFgJmPpdyTeTWf1ehBHaBu5mQTTEY4iW5RkCyRA9pncmBKC6qZ-SLgeWEHgRS0PDUBf6m6dRygXk4aYNu97dOiPEP7O5e9COgvcVsyfpzHJeEAC7aJTb15_5ZGUXNGFWZe6THN4PifqUUmdkxoQVtfJ4HgHnL_ZUV3EmZz93Ld7LKRxlK3dGzvzTNdNo22QJ1PCo3qkPlzZ-BEpGGiwCwu10qwuAEbXcQXk8HTh4WMDO6g5KBojMDGSPkTGVRRUyuKDGEl9ybmFavp5Uerh1mb5QO1rv9XZiaX2XWertmvdwluZPmuWjomKXPpUafJ81TVUlPMHG0cf4aO_QvPaUugw5UmEYaiBvIphBKre--5Od-Few9ZsX8bEuaLQF75hN1c75xLKOXhAEq66DQn8YehNYHlj1Kah4wq0YJREwJDyloIbvBv0ufE7bTJikqLsW9saavHhA0p78_Et2dUPL7B9kEs_T5ZvxjSrFfnG89ROGzioNBlsQnNOhPwTyiNvhgfN-9SbtC_iMw6Du4uZ8UGsr-0G00)

### Recalculation request handling with multiple Partitions
- Multiple InputProcessors
- Multiple DataProcessors
- any DataProcessor request for refresh will be sent to all InputProcessors
- all InputProcessors will tell all DataProcessors that it is starting a refresh

### Recalculation with change to ContextKey schema
Multiple DataProcessors, one of them requests refresh (send to all inputProcessors), all InputProcessors refresh to all DataProcessors
- InputProcessors send "RefreshStarting" message to DataProcessors, so even if DP hasn't requested refresh yet, it will know a refresh is happening and react correctly
