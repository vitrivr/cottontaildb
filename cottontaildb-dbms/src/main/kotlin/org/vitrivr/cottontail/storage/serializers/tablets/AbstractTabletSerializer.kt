package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.apache.lucene.util.compress.LZ4
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.Tablet
import org.vitrivr.cottontail.utilities.math.BitUtil.isBitSet
import org.vitrivr.cottontail.utilities.math.BitUtil.setBit
import org.xerial.snappy.BitShuffle
import org.xerial.snappy.BitShuffleType
import org.xerial.snappy.Snappy
import java.nio.ByteBuffer

/**
 * An abstract implementation of a [TabletSerializer].
 *
 * It mainly takes care of the compression and decompression of the data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractTabletSerializer<T: Value>(final override val type: Types<T>): TabletSerializer<T> {

    /** Internal buffer used to store uncompressed value data. */
    protected val dataBuffer = ByteBuffer.allocateDirect(8 + this.type.physicalSize * TabletSerializer.DEFAULT_SIZE)

    /** Internal buffer used to store compressed data. */
    protected val compressBuffer = ByteBuffer.allocateDirect(Snappy.maxCompressedLength(this.dataBuffer.capacity()))

    /** Internal [ArrayList] containing the output values. */
    private val outputBuffer = arrayOfNulls<Value?>(TabletSerializer.DEFAULT_SIZE)

    /**
     * Deserializes a [Tablet] of type [FloatVectorValue] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [Tablet].
     */
    final override fun fromEntry(entry: ByteIterable): Tablet<T> {
        /* Transfer data into buffer. */
        this.compressBuffer.clear()
        for (b in entry) { this.compressBuffer.put(b) }

        /* Uncompress data using snappy. */
        Snappy.uncompress(this.compressBuffer.flip(), this.dataBuffer.clear())

        /* Materialize data from data buffer. */
        val map = this.dataBuffer.clear().long
        for (index in 0 until TabletSerializer.DEFAULT_SIZE) {
            if (map.isBitSet(index)) {
                this.outputBuffer[index] = this.readFromBuffer()
            } else {
                this.outputBuffer[index] = null
            }
        }
        return Tablet(this.type, this.outputBuffer)
    }

    /**
     * Serializes a [Tablet] of type [DoubleVectorValue] to create a [ByteIterable].
     *
     * @param tablet The [Tablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    final override fun toEntry(tablet: Tablet<T>): ByteIterable {
        /* Write non-null values to buffer and mark them in bitmap. */
        var map = 0L
        this.dataBuffer.position(8) /* Move to 8-th byte, this is where we start. */
        for (index  in 0 until tablet.size) {
            val value = tablet[index]
            if (value != null) {
                map = map.setBit(index)
                this.writeToBuffer(value)
            }
        }

        /* Write bit-map to beginning of buffer. */
        this.dataBuffer.putLong(0, map)

        /* Compress data and return it as ArrayByteIterable. */
        val compressedSize = Snappy.compress(this.dataBuffer.flip(), this.compressBuffer.clear())
        return ArrayByteIterable(ByteArray(compressedSize) { this.compressBuffer[it] })
    }

    /**
     * Writes an individual value of type [T] to the internal [dataBuffer].
     *
     * @param value The value to write.
     */
    protected abstract fun writeToBuffer(value: T)

    /**
     * Reads an individual value of type [T] from the internal [dataBuffer].
     *
     * @return [Value] of type [T]
     */
    protected abstract fun readFromBuffer(): T

}