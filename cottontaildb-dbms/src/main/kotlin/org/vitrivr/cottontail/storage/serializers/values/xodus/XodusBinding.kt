package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value

/**
 * A serializer for Xodus based [Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface XodusBinding<T: Value> {

    /** The [Types] converted by this [XodusBinding]. */
    val type: Types<T>

    /**
     * Converts a [ByteIterable] to a [Value].
     *
     * @param entry The [ByteIterable] to convert.
     * @return The resulting [Value].
     */
    fun entryToValue(entry: ByteIterable): T?

    /**
     * Converts a [Value] to a [ByteIterable].
     *
     * @param value The [Value] to convert.
     * @return The resulting [ByteIterable].
     */
    fun valueToEntry(value: T?): ByteIterable
}