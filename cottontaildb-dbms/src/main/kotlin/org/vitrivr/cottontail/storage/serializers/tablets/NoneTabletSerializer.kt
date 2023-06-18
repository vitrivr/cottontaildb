package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.tablets.AbstractTablet
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * An implementation of a [TabletSerializer] that uses no compression of values.
 *
 * Can be a good choice if data cannot be compressed efficiently.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class NoneTabletSerializer<T: Value>(override val type: Types<T>, val size: Int): TabletSerializer<T> {

    /**
     * Deserializes a [AbstractTablet] of type [FloatVectorValue] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [AbstractTablet].
     */
    override fun fromEntry(entry: ByteIterable): Tablet<T> {
        /* Transfer data into buffer. */
        val tablet = Tablet.of(this.size, this.type)
        for (b in entry) {
            tablet.buffer.put(b)
        }
        return tablet
    }

    /**
     * Serializes a [AbstractTablet] of type [DoubleVectorValue] to create a [ByteIterable].
     *
     * @param tablet The [AbstractTablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    override fun toEntry(tablet: Tablet<T>): ByteIterable = ArrayByteIterable(ByteArray(tablet.buffer.clear().limit()) {
        tablet.buffer.get()
    })
}