package org.vitrivr.cottontail.dbms.execution.transactions

/**
 * Type of [TransactionManager.TransactionImpl].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class TransactionType(val autoCommit: Boolean, val autoRollback: Boolean, val readonly: Boolean) {
    USER(false, false, false),                  /* A [Transaction] created by a user. */
    USER_IMPLICIT(true, true, false),           /* A [Transaction] created by a user that, is bound to the context of a single query. */
    USER_IMPLICIT_READONLY(true, true, true),   /* A [Transaction] created by a user that, is bound to the context of a single query and is used in a read-only fashion. */
    SYSTEM(false, false, false),                /* A [Transaction] created by the system. */
    SYSTEM_READONLY(false, false, true)         /* A [Transaction] created by the system and is used in a read-only fashion . */
}