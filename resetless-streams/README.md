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

# Basic topology (single input, single processor)
![Basic topology](https://www.plantuml.com/plantuml/png/dP2nJiGm44HxVyMM8775jIdG5JI80iGX-bMyXKLYh-pnIUZ4lnElKiI7qb1PrhoypFXKm1brdfmkPnY3SWGzHwtuI1f6ua81VyEtd4Of9MK3j0FUnMu8AROqsxKZdXZS6NnlUAtGYZEoyDpYL9mBeCCZ1Hte-YNVnmUwy5Jb-EDYWb2wI6uzYCvWoE3e5fFegQ6BIPjfhMM7gRrkLBvJtOMs-PwkhVYBQNVadMAWTIGTQCp2a7NPfttc_ly_Go1v5XyOqf7Rm8o6fueRPTTbEy7R-wGoxCEdXQxpz0i0)
