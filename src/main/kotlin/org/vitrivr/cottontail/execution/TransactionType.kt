package org.vitrivr.cottontail.execution

/**
 * Type of [TransactionManager.TransactionImpl].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class TransactionType(val autoCommit: Boolean, val autoRollback: Boolean) {
    USER(false, false),              /* A [Transaction] created by a user. */
    USER_IMPLICIT(true, true),       /* A [Transaction] created by a user that, is bound to the context of a single query. */
    SYSTEM(true, true)               /* A [Transaction] created by the system. */
}