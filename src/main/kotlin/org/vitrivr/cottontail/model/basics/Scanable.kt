package org.vitrivr.cottontail.model.basics

/**
 * An objects that holds [Record] values and allows for scanning operation on those [Record] values.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.2
 */
interface Scanable {
    /**
     * Returns an [Iterator] for all the [TupleId]s in this [Scanable].
     *
     * @return action Iterator<TupleId>
     */
    fun scan(): CloseableIterator<TupleId>

    /**
     * Returns an [Iterator] for all the [TupleId]s contained in the provide [LongRange]
     * and this [Scanable]. Can be used for partitioning.
     *
     * @param range The [LongRange] to iterate over
     * @return action Iterator<TupleId>
     */
    fun scan(range: LongRange): CloseableIterator<TupleId>
}