package org.vitrivr.cottontail.dbms.index

/**
 * An [Index] configuration object.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface IndexConfig<T: Index> : Comparable<IndexConfig<T>> {
    /**
     * Meaningless comparison but required by Xodus.
     */
    override fun compareTo(other: IndexConfig<T>): Int = this.hashCode().compareTo(other.hashCode())
}