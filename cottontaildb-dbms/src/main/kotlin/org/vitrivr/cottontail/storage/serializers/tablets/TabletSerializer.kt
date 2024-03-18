package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ByteIterable
import net.jpountz.lz4.LZ4Factory
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.tablets.AbstractTablet
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A serializer for Xodus based [Value] [AbstractTablet] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface TabletSerializer<T: Value> {

    /** The [Types] converted by this [TabletSerializer]. */
    val type: Types<T>

    /**
     * Deserializes a [AbstractTablet] of type [T] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [AbstractTablet].
     */
    fun fromEntry(entry: ByteIterable): Tablet<T>

    /**
     * Serializes a [AbstractTablet] of type [T] to create a [ByteIterable].
     *
     * @param tablet The [AbstractTablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    fun toEntry(tablet: Tablet<T>): ByteIterable
}