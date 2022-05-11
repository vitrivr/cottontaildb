package org.vitrivr.cottontail.dbms.index

/**
 * The state of an [IndexRebuilder].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class IndexRebuilderState {
    INITIALIZED,
    SCANNED,
    MERGED,
    ABORTED,
    CLOSED
}