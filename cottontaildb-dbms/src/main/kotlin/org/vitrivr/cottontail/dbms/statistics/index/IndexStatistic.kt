package org.vitrivr.cottontail.dbms.statistics.index

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.statistics.storage.ColumnStatistic
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.statistics.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A statistics item for an index.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class IndexStatistic(val key: String, val value: String, val updated: Long = System.currentTimeMillis()): Comparable<IndexStatistic> {
    companion object {
        /**
         * De-serializes a [IndexStatistic] from the given [ByteIterable].
         *
         * @param entry The [ByteIterable] to read entry from.
         */
        fun entryToObject(entry: ByteIterable): IndexStatistic {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            return IndexStatistic(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), LongBinding.BINDING.readObject(stream))
        }

        /**
         * Serializes a [IndexStatistic] to a [ArrayByteIterable].
         *
         * @param `object` The [IndexStatistic] to serialize.
         * @return [ArrayByteIterable]
         */
        fun objectToEntry(`object`: IndexStatistic): ArrayByteIterable {
            val output = LightOutputStream()
            StringBinding.BINDING.writeObject(output, `object`.key)
            StringBinding.BINDING.writeObject(output, `object`.value)
            LongBinding.BINDING.writeObject(output, `object`.updated)
            return output.asArrayByteIterable()
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