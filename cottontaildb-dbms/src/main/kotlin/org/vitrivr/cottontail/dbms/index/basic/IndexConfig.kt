package org.vitrivr.cottontail.dbms.index.basic

/**
 * An [Index] configuration object.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface IndexConfig<T: Index> : Comparable<IndexConfig<T>> {

    /**
     * Converts this [IndexConfig] to a [Map] of key-value pairs.
     *
     * @return [Map]
     */
    fun toMap(): Map<String,String>

    /**
     * Meaningless comparison but required by Xodus.
     */
    override fun compareTo(other: IndexConfig<T>): Int = this.hashCode().compareTo(other.hashCode())
}