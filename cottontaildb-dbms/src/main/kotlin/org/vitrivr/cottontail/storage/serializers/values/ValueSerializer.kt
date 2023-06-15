package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value

/**
 * A serializer for Xodus based [Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
sealed interface ValueSerializer<T: Value> {

    /** The [Types] converted by this [ValueSerializer]. */
    val type: Types<T>

    /**
     * Converts a [ByteIterable] to a [Value].
     *
     * @param entry The [ByteIterable] to convert.
     * @return The resulting [Value].
     */
    fun fromEntry(entry: ByteIterable): T?

    /**
     * Converts a [Value] to a [ByteIterable].
     *
     * @param value The [Value] to convert.
     * @return The resulting [ByteIterable].
     */
    fun toEntry(value: T?): ByteIterable
}