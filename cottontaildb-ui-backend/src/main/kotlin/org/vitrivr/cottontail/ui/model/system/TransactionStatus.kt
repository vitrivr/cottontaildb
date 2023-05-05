package org.vitrivr.cottontail.ui.model.system

import kotlinx.serialization.Serializable

/**
 * Enumeration of the status of a [Transaction].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
enum class TransactionStatus {
    IDLE,
    RUNNING,
    FINALIZING,
    ERROR,
    COMMIT,
    ROLLBACK,
}