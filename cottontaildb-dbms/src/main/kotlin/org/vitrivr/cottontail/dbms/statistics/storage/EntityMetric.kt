package org.vitrivr.cottontail.dbms.statistics.storage

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream

/**
 * A metric describing an entity in the database.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class EntityMetric(var inserts: Long = 0L, var updates: Long = 0L, var deletes: Long = 0L, var deltaSinceAnalysis: Long = 0L, var lastAnalysis: Long = 0L) {

    /** The estimated number of entries in the entity backed by this [EntityMetric]. */
    val total: Long
        get() = this.inserts - this.deletes

    companion object {
        fun entryToObject(entry: ByteIterable): EntityMetric {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            return EntityMetric(
                LongBinding.readCompressed(stream),
                LongBinding.readCompressed(stream),
                LongBinding.readCompressed(stream),
                LongBinding.readCompressed(stream)
            )
        }

        fun objectToEntry(`object`: EntityMetric): ArrayByteIterable {
            val output = LightOutputStream()
            LongBinding.writeCompressed(output, `object`.inserts)
            LongBinding.writeCompressed(output, `object`.updates)
            LongBinding.writeCompressed(output, `object`.deletes)
            LongBinding.writeCompressed(output, `object`.deltaSinceAnalysis)
            LongBinding.writeCompressed(output, `object`.lastAnalysis)
            return output.asArrayByteIterable()
        }
    }
}