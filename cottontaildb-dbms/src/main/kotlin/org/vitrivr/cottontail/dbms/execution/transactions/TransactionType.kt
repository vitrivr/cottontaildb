package org.vitrivr.cottontail.dbms.execution.transactions

/**
 * Type of [TransactionManager.TransactionImpl].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class TransactionType(val autoCommit: Boolean, val autoRollback: Boolean, val readonly: Boolean, val exclusive: Boolean) {
    USER_EXCLUSIVE(false, false, false, true),         /* An exclusive transaction created by a user.  */
    USER_READONLY(false, false, true, false),          /* A read-only transaction created by a user.  */
    USER_IMPLICIT_EXCLUSIVE(true, true, false, true),  /* An exclusive transaction created by a user that is bound to the context of a single query. */
    USER_IMPLICIT_READONLY(true, true, true, false),   /* A read-only transaction created by a user that is bound to the context of a single query. */
    SYSTEM_EXCLUSIVE(false, false, false, true),       /* An exclusive transaction created by the system. */
    SYSTEM_READONLY(false, false, true, false)         /* A read-only transaction created by the system. */
}