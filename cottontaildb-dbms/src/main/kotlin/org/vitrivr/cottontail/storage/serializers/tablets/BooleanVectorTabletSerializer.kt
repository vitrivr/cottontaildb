package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.utilities.math.BitUtil.isBitSet
import org.vitrivr.cottontail.utilities.math.BitUtil.setBit
import java.nio.ByteBuffer
import kotlin.math.sign

class BooleanVectorTabletSerializer(size: Int): TabletSerializer<BooleanVectorValue> {

    /** The [Types] converted by this [TabletSerializer]. */
    override val type = Types.BooleanVector(size)

    /** Internal buffer used to store value data. */
    private val dataBuffer = ByteBuffer.allocate(8 + TabletSerializer.DEFAULT_SIZE * type.physicalSize)

    /** Internal [ArrayList] containing the output values. */
    private val outputBuffer = arrayOfNulls<Value?>(TabletSerializer.DEFAULT_SIZE)

    /**
     * Deserializes a [Tablet] of type [BooleanVectorValue] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [Tablet].
     */
    final override fun fromEntry(entry: ByteIterable): Tablet<BooleanVectorValue> {
        TODO()
    }

    /**
     * Serializes a [Tablet] of type [BooleanVectorValue] to create a [ByteIterable].
     *
     * @param tablet The [Tablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    override fun toEntry(tablet: Tablet<BooleanVectorValue>): ByteIterable {
        TODO()
    }
}