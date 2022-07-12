package org.vitrivr.cottontail.dbms.index.basic

/**
 * The state of an [Index] in Cottontail DB. Acts as a hint to the query planner whether an index should be used for query execution.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class IndexState {
    CLEAN, /** The [Index] is clean, up-to-date and ready for use. */
    DIRTY, /** The [Index] is dirty and using it may incur additional costs in terms for performance and/or quality. */
    STALE /** The [Index] is stale and cannot be used any longer. */
}