package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ByteBufferByteIterable
import jetbrains.exodus.ByteIterable
import net.jpountz.lz4.LZ4Compressor
import net.jpountz.lz4.LZ4FastDecompressor
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

    /** Internal [ArrayList] containing the output values. */
    private val outputBuffer = arrayOfNulls<Value?>(TabletSerializer.DEFAULT_SIZE)

    /** The [BitShuffleType] used by this [AbstractTabletSerializer]. */
    private val bitShuffleType = when(this.type) {
        Types.Byte -> BitShuffleType.BYTE
        Types.Boolean,
        is Types.BooleanVector,
        Types.Long,
        Types.Date,
        is Types.LongVector -> BitShuffleType.LONG
        Types.Float,
        Types.Complex32,
        is Types.FloatVector,
        is Types.Complex32Vector -> BitShuffleType.FLOAT
        Types.Int,
        is Types.IntVector -> BitShuffleType.INT
        Types.Short -> BitShuffleType.SHORT
        Types.Double,
        Types.Complex64,
        is Types.DoubleVector,
        is Types.Complex64Vector -> BitShuffleType.DOUBLE
        else -> throw  IllegalArgumentException("Type $type is not supported for tablet serialization.")
    }

    /** Internal buffer used to store uncompressed value data. */
    protected val dataBuffer: ByteBuffer = ByteBuffer.allocateDirect(this.type.physicalSize * TabletSerializer.DEFAULT_SIZE)

    /** Internal buffer used to store uncompressed value data. */
    protected val shuffleBuffer: ByteBuffer = ByteBuffer.allocateDirect(this.type.physicalSize * TabletSerializer.DEFAULT_SIZE)

    /** Internal buffer used to store compressed data. */
    private val compressBuffer: ByteBuffer = ByteBuffer.allocateDirect(8 + Snappy.maxCompressedLength(this.dataBuffer.capacity()))

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
        val map = this.compressBuffer.flip().getLong()

        /* Uncompress payload using LZ4. */
        Snappy.uncompress(this.compressBuffer, this.shuffleBuffer.clear())
        BitShuffle.unshuffle(this.shuffleBuffer.flip(), this.bitShuffleType, this.dataBuffer.clear())

        /* Materialize data from data buffer. */
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
        this.dataBuffer.clear()
        for (index  in 0 until tablet.size) {
            val value = tablet[index]
            if (value != null) {
                map = map.setBit(index)
                this.writeToBuffer(value)
            }
        }

        /* Compress data and return it as ArrayByteIterable. */
        BitShuffle.shuffle(this.dataBuffer.flip(), this.bitShuffleType, this.shuffleBuffer.clear())
        this.compressBuffer.clear().putLong(map) /* Write decompressed length at the beginning. */
        Snappy.compress(this.shuffleBuffer, this.compressBuffer)
        return ByteBufferByteIterable(this.compressBuffer.position(0))
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