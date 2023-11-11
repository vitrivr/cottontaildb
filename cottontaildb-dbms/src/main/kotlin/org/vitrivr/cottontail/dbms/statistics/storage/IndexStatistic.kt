package org.vitrivr.cottontail.dbms.statistics.storage

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import java.io.ByteArrayInputStream

/**
 * A statistics item for an index.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class IndexStatistic(private val map: Map<String,String>) {

    companion object {
        fun deserialize(iterable: ByteIterable): IndexStatistic {
            val stream = ByteArrayInputStream(iterable.bytesUnsafe, 0, iterable.length)
            val size = IntegerBinding.BINDING.readObject(stream)
            val map = mutableMapOf<String,String>()
            for (i in 0 until size) {
                map[StringBinding.BINDING.readObject(stream)] = StringBinding.BINDING.readObject(stream)
            }
            return IndexStatistic(map)
        }

        fun serialize(statistic: IndexStatistic): ByteIterable {
            val output = LightOutputStream()
            IntegerBinding.BINDING.writeObject(output, statistic.map.size)
            for ((key, value) in statistic.map) {
                StringBinding.BINDING.writeObject(output, key)
                StringBinding.BINDING.writeObject(output, value)
            }
            return output.asArrayByteIterable()
        }
    }

    /**
     * Tries to fetch [key] and convert it to [Int].
     *
     * @param key The key to fetch.
     * @return [Int] representation value associated with [key]
     */
    fun asInt(key: String): Int = this.map[key]?.toInt() ?: throw IllegalArgumentException("Key $key does not exist on index statistic.")

    /**
     * Tries to fetch [key] and convert it to [Long].
     *
     * @param key The key to fetch.
     * @return [Long] representation value associated with [key]
     */
    fun asLong(key: String): Long =  this.map[key]?.toLong() ?: throw IllegalArgumentException("Key $key does not exist on index statistic.")

    /**
     * Tries to fetch [key] and convert it to [Float].
     *
     * @param key The key to fetch.
     * @return [Float] representation value associated with [key]
     */
    fun asFloat(key: String): Float = this.map[key]?.toFloat() ?: throw IllegalArgumentException("Key $key does not exist on index statistic.")

    /**
     * Tries to fetch [key] and convert it to [Double].
     *
     * @param key The key to fetch.
     * @return [Double] representation value associated with [key]
     */
    fun asDouble(key: String): Double =  this.map[key]?.toDouble() ?: throw IllegalArgumentException("Key $key does not exist on index statistic.")
}