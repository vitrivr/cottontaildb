package org.vitrivr.cottontail.dbms.statistics.Index

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import java.io.ByteArrayInputStream

/**
 * A statistics item for an index.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class IndexStatistic(val key: String, val updated: Long, val value: String): Comparable<IndexStatistic> {


    companion object: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<IndexStatistic> = IndexStatistic(
            StringBinding.BINDING.readObject(stream),
            LongBinding.BINDING.readObject(stream),
            StringBinding.BINDING.readObject(stream),
        )
        override fun writeObject(output: LightOutputStream, `object`: Comparable<IndexStatistic>) {
            require(`object` is IndexStatistic) { "IndexStatistic.Binding can only be used to serialize instances of IndexStatistic." }
            StringBinding.BINDING.writeObject(output, `object`.key)
            LongBinding.BINDING.writeObject(output, `object`.updated)
            StringBinding.BINDING.writeObject(output, `object`.value)
        }
    }

    /**
     * Tries to interpret and returns this [value] as [Int].
     *
     * @return [Int] representation of this [value]
     */
    fun asInt(): Int = this.value.toInt()

    /**
     * Tries to interpret and returns this [value] as [Long].
     *
     * @return [Long] representation of this [value]
     */
    fun asLong(): Long = this.value.toLong()

    /**
     * Tries to interpret and returns this [value] as [Float].
     *
     * @return [Float] representation of this [value]
     */
    fun asFloat(): Float = this.value.toFloat()

    /**
     * Tries to interpret and returns this [value] as [Double].
     *
     * @return [Double] representation of this [value]
     */
    fun asDouble(): Double = this.value.toDouble()

    override fun compareTo(other: IndexStatistic): Int
        = this.key.compareTo(other.key)
}