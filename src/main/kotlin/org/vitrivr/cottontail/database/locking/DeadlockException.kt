package org.vitrivr.cottontail.database.locking

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class DeadlockException constructor(e: Exception) : Exception(e)