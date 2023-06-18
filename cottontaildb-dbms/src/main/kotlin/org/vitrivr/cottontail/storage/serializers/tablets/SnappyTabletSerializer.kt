package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ByteBufferByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.tablets.AbstractTablet
import org.vitrivr.cottontail.core.values.tablets.Tablet
import org.xerial.snappy.BitShuffle
import org.xerial.snappy.BitShuffleType
import org.xerial.snappy.Snappy
import java.nio.ByteBuffer

/**
 * An implementation of a [TabletSerializer] that uses Snappy compression for values.
 *
 * Potentially slower than [LZ4TabletSerializer], however, can yield better compression for high-entropy data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SnappyTabletSerializer<T: Value>(override val type: Types<T>, val size: Int): TabletSerializer<T> {


    /** Internal buffer used to store bit-shuffled data. */
    private val shuffleBuffer: ByteBuffer = when (type) {
        Types.Boolean -> ByteBuffer.allocateDirect(this.size shr 3)
        is Types.BooleanVector -> ByteBuffer.allocateDirect((this.size * this.type.logicalSize).shr(3) + Int.SIZE_BYTES)
        else -> ByteBuffer.allocateDirect(this.size * this.type.physicalSize)
    }

    /** Internal buffer used to store compressed data. */
    private val compressBuffer: ByteBuffer = ByteBuffer.allocateDirect((this.size shr 3) + Snappy.maxCompressedLength(this.shuffleBuffer.capacity()))

    /** The [BitShuffleType] used by this [SnappyTabletSerializer]. */
    private val bitShuffleType = when(this.type) {
        Types.Byte -> BitShuffleType.BYTE
        Types.Boolean,
        Types.Long,
        Types.Date,
        is Types.LongVector -> BitShuffleType.LONG
        Types.Float,
        Types.Complex32,
        is Types.FloatVector,
        is Types.Complex32Vector -> BitShuffleType.FLOAT
        Types.Int,
        is Types.IntVector,
        is Types.BooleanVector -> BitShuffleType.INT
        Types.Short -> BitShuffleType.SHORT
        Types.Double,
        Types.Complex64,
        is Types.DoubleVector,
        is Types.Complex64Vector -> BitShuffleType.DOUBLE
        else -> throw  IllegalArgumentException("Type $type is not supported for tablet serialization.")
    }

    /**
     * Deserializes a [Tablet] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [AbstractTablet].
     */
    override fun fromEntry(entry: ByteIterable): Tablet<T> {
        /* Transfer data into buffer. */
        this.compressBuffer.clear()
        for (b in entry) { this.compressBuffer.put(b) }

        /* Create new tablet and write prefix. */
        val tablet = Tablet.of(this.size, this.type, true)
        tablet.buffer.put(0, this.compressBuffer.flip(), 0, this.size shr 3)

        /* Uncompress payload. */
        Snappy.uncompress(this.compressBuffer.position(this.size shr 3), this.shuffleBuffer.clear())

        /* Unshuffle data into byte buffer. */
        BitShuffle.unshuffle(this.shuffleBuffer, this.bitShuffleType, tablet.buffer.slice(this.size shr 3, this.shuffleBuffer.capacity()))
        tablet.buffer.clear()

        return tablet
    }

    /**
     * Serializes a [Tablet] to create a [ByteIterable].
     *
     * @param tablet The [Tablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    override fun toEntry(tablet: Tablet<T>): ByteIterable {
        /* Write prefix to buffer. */
        this.compressBuffer.clear().put(0, tablet.buffer.clear(), 0, this.size shr 3)

        /* Shuffle data into byte buffer. */
        BitShuffle.shuffle(tablet.buffer.position(this.size shr 3), this.bitShuffleType, this.shuffleBuffer.clear())

        /* Compress data. */
        Snappy.compress(this.shuffleBuffer, this.compressBuffer.position(this.size shr 3))
        tablet.buffer.clear()

        return ByteBufferByteIterable(this.compressBuffer.position(0))
    }
}