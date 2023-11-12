package org.vitrivr.cottontail.dbms.execution.transactions

import org.vitrivr.cottontail.dbms.execution.locking.LockMode

/**
 * The type of access to a object.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class AccessMode(val lock: LockMode) {
    READ(LockMode.SHARED),
    WRITE(LockMode.EXCLUSIVE)
}