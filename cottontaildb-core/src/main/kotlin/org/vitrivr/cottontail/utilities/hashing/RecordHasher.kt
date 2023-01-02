package org.vitrivr.cottontail.utilities.hashing

import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing
import org.vitrivr.cottontail.core.database.ColumnDef
import java.nio.ByteBuffer

/**
 * This utility class can be used to generate a hash value from a [org.vitrivr.cottontail.core.basics.Record], taking into account specific [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RecordHasher(val columns: List<ColumnDef<*>>, val seed: Long = System.currentTimeMillis()) {

    /** The [Array] of [HashFunction]s used for this [RecordHasher]. */
    private val hashers: Array<HashFunction> = this.columns.map { Hashing.murmur3_128(this.seed.toInt()) }.toTypedArray()

    /**
     * Returns a [ByteArray] containing the hash code for the given [org.vitrivr.cottontail.core.basics.Record].
     *
     * @param record The [org.vitrivr.cottontail.core.basics.Record] to hash.
     * @return The hash code.
     */
    fun hash(record: org.vitrivr.cottontail.core.basics.Record): ByteArray {
        val buffer = ByteBuffer.allocate(16 * this.hashers.size)
        for ((i, c) in this.columns.withIndex()) {
            val code = this.hashers[i].hashObject(record[c], ValueFunnel)
            buffer.put(code.asBytes())
        }
        return buffer.array()
    }
}