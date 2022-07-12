package org.vitrivr.cottontail.dbms.index.basic.rebuilder

/**
 * The state of an [AbstractAsyncIndexRebuilder].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class IndexRebuilderState {
    INITIALIZED,

    SCANNING,

    SCANNED,

    MERGING,

    MERGED,

    ABORTED,

    FINISHED
}