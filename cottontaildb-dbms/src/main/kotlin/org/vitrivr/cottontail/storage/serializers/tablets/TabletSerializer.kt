package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A serializer for Xodus based [Value] [Tablet] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface TabletSerializer<T: Value> {

    companion object {
        /** The size of [Tablet]s that can be serialized. */
        const val DEFAULT_SIZE = Long.SIZE_BITS
    }

    /** The [Types] converted by this [TabletSerializer]. */
    val type: Types<T>

    /**
     * Deserializes a [Tablet] of type [T] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [Tablet].
     */
    fun fromEntry(entry: ByteIterable): Tablet<T>

    /**
     * Serializes a [Tablet] of type [T] to create a [ByteIterable].
     *
     * @param tablet The [Tablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    fun toEntry(tablet: Tablet<T>): ByteIterable
}