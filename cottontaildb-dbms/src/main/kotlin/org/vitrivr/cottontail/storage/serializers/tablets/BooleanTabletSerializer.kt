package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.Tablet
import org.vitrivr.cottontail.storage.serializers.tablets.TabletSerializer.Companion.DEFAULT_SIZE
import org.vitrivr.cottontail.utilities.math.BitUtil.isBitSet
import org.vitrivr.cottontail.utilities.math.BitUtil.setBit
import java.nio.ByteBuffer

/**
 * An implementation of a [TabletSerializer] for [BooleanValue], which require special treatment, since
 * an individual value is smaller than one byte.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BooleanTabletSerializer: TabletSerializer<BooleanValue> {

    /** The [Types] converted by this [TabletSerializer]. */
    override val type: Types<BooleanValue> = Types.Boolean

    /** Internal buffer used to store value data. */
    private val dataBuffer = ByteBuffer.allocate(16)

    /** Internal [ArrayList] containing the output values. */
    private val outputBuffer = arrayOfNulls<Value?>(TabletSerializer.DEFAULT_SIZE)

    /**
     * Deserializes a [Tablet] of type [BooleanValue] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [Tablet].
     */
    override fun fromEntry(entry: ByteIterable): Tablet<BooleanValue> {
        /* Transfer data into buffer. */
        this.dataBuffer.clear()
        for (b in entry) {
            this.dataBuffer.put(b)
        }

        /* Materialize data from data buffer. */
        val map = this.dataBuffer.clear().long
        val value = this.dataBuffer.long
        for (index in 0 until DEFAULT_SIZE) {
            if (map.isBitSet(index)) {
                this.outputBuffer[index] = BooleanValue(value.isBitSet(index))
            } else {
                this.outputBuffer[index] = null
            }
        }

        /* Return tablet. */
        return Tablet(this.type, this.outputBuffer)
    }

    /**
     * Serializes a [Tablet] of type [BooleanValue] to create a [ByteIterable].
     *
     * @param tablet The [Tablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    override fun toEntry(tablet: Tablet<BooleanValue>): ByteIterable {
        var map = 0L
        var values = 0L
        for (index in 0 until tablet.size) {
            val value = tablet[index]
            if (value != null) {
                map = map.setBit(index)
                if (value.value) {
                    values = values.setBit(index)
                }
            }
        }

        /* Write bit-map to beginning of buffer. */
        this.dataBuffer.clear().putLong(map)
        this.dataBuffer.putLong(values)

        /* Compress data and return it as ArrayByteIterable. */
        return ArrayByteIterable(this.dataBuffer.array())
    }
}