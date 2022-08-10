package org.vitrivr.cottontail.dbms.index.basic.rebuilder

/**
 * The state of an [AbstractAsyncIndexRebuilder].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class IndexRebuilderState(val trackChanges: Boolean) {
    INITIALIZED(false),

    SCANNING(true),

    SCANNED(true),

    MERGING(false),

    MERGED(false),

    ABORTED(false),

    FINISHED(false)
}