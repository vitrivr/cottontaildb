# Change log for Cottontail DB

## Version 0.12.0

### General changes

* <ins>Breaking:</ins> Re-organized catalogue; old instances of Cottontail DB cannot be used with version 0.12.0. Use _migration_ tool migrate an instance.
* Language: Added support for ORDER BY-clauses and sub-SELECTS.
* Language: NNS queries now only support a single query / weight vector. Batched NNS will be re-added in a future version with batched query support.
* Language: All commands with the exception of _ping_, _begin_, _commit_, _rollback_ now return _QueryResponseMessages_
* CLI: Re-organized and extended existing CLI functionality.
* CLI: The _system_ command now hosts commands that allow for introspection into the Cottontail DB execution engine
* CLI: Added a _system migration_ command that allow for migration of old catalogue versions.
* Added transaction support; we use 2PL internally to maintain locks on database objects and enforce isolation
* Added support for incremental updates to indexes upon INSERT, UPDATE or DELETE, if an indexes supports this (#10)
* Added PQ and VA indexes for nearest neighbor search
* Added column statistics that can be used for query planning
* Re-added support for SELECT DISTINCT projectsion (#68)
* Re-structured query planner, which should now scale better to more complex query plans

### Bugfixes

* The results of entity optimization are now persistent and visible once the operation completes (#67)
