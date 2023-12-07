package org.vitrivr.cottontail.dbms.index.basic.rebuilder

/**
 * The state of an [AbstractAsyncIndexRebuilder].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class IndexRebuilderState {
    /** Index rebuilder is ready and waiting for the process to start. */
    INITIALIZED,

    /**Index is currently being rebuilt. */
    REBUILDING,

    /** Index has been rebuilt and is waiting for replacement phase to start.*/
    REBUILT,

    /** Index is being replaced. */
    REPLACING,

    /** Index rebuilding has been aborted. */
    ABORTED,

    /** Index rebuilding has been completed successfully. */
    FINISHED
}