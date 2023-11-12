package org.vitrivr.cottontail.dbms.execution.transactions

/**
 * Type of [Transaction].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class TransactionType {
    SNAPSHOT_READONLY,
    SERIALIZABLE_READONLY,
    SNAPSHOT,
    SERIALIZABLE
}