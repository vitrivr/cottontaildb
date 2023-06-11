package org.vitrivr.cottontail.utilities.hashing

import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.Tuple
import java.nio.ByteBuffer

/**
 * This utility class can be used to generate a hash value from a [org.vitrivr.cottontail.core.basics.Tuple], taking into account specific [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RecordHasher(val columns: List<ColumnDef<*>>, val seed: Long = System.currentTimeMillis()) {

    /** The [Array] of [HashFunction]s used for this [RecordHasher]. */
    private val hashers: Array<HashFunction> = this.columns.map { Hashing.murmur3_128(this.seed.toInt()) }.toTypedArray()

    /**
     * Returns a [ByteArray] containing the hash code for the given [org.vitrivr.cottontail.core.basics.Tuple].
     *
     * @param tuple The [org.vitrivr.cottontail.core.basics.Tuple] to hash.
     * @return The hash code.
     */
    fun hash(tuple: Tuple): ByteArray {
        val buffer = ByteBuffer.allocate(16 * this.hashers.size)
        for ((i, c) in this.columns.withIndex()) {
            val code = this.hashers[i].hashObject(tuple[c], ValueFunnel)
            buffer.put(code.asBytes())
        }
        return buffer.array()
    }
}