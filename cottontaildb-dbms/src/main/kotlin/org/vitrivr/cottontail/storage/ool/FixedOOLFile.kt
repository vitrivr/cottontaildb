package org.vitrivr.cottontail.storage.ool

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.OutOfLineValue
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.storage.ool.interfaces.AccessPattern
import org.vitrivr.cottontail.storage.ool.interfaces.AccessPattern.RANDOM
import org.vitrivr.cottontail.storage.ool.interfaces.AccessPattern.SEQUENTIAL
import org.vitrivr.cottontail.storage.ool.interfaces.OOLFile
import org.vitrivr.cottontail.storage.ool.interfaces.OOLFile.Companion.SEGMENT_SIZE
import org.vitrivr.cottontail.storage.ool.interfaces.OOLReader
import java.lang.Math.floorDiv
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.jvm.optionals.getOrElse

/**
 * A [FixedOOLFile] only support fixed-length records.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FixedOOLFile<V: Value>(path: Path, type: Types<V>): AbstractOOLFile<V, OutOfLineValue.Fixed>(path, type) {
    /** The size of a segment in bytes. */
    val entryPerSegment: Int = (floorDiv(SEGMENT_SIZE, this.type.physicalSize) + 1)

    /** The size of a segment in bytes. */
    val segmentSizeBytes: Int = this.entryPerSegment  * this.type.physicalSize

    /** An internal counter of the highest segment ID. */
    override var appendSegmentId: Long = 0L

    init {
        val maxSegment =  Files.list(this.path).map { it.fileName.toString() }.filter { it.endsWith(".ool") }.map { it.substringBefore('.').toLong() }.max { o1, o2 -> o1.compareTo(o2) }.getOrElse { 0L }
        val maxSegmentPath = this.path.resolve("$maxSegment.ool")
        if (!Files.exists(maxSegmentPath) || Files.size(maxSegmentPath) < this.segmentSizeBytes) {
            this.appendSegmentId = maxSegment
        } else {
            this.appendSegmentId = maxSegment + 1
        }
    }

    /**
     * Provides a [OOLReader] for this [OOLFile].
     *
     * @param pattern [AccessPattern] to use for reading.
     * @return [OOLReader]
     */
    override fun reader(pattern: AccessPattern) = when(pattern) {
        SEQUENTIAL -> SequentialReader()
        RANDOM -> RandomReader()
    }

    /**
     * Creates a new [Writer] for this [AbstractOOLFile].
     *
     * @return New [Writer].
     */
    override fun newWriter() = Writer()

    /**
     * A [SequentialReader] for a [FixedOOLFile].
     *
     * The [SequentialReader] (pre-)fetches entries in larger segments and caches them in memory to minimize latency.
     */
    inner class SequentialReader: OOLReader<V, OutOfLineValue.Fixed> {
        /** Reference to the [OOLFile] this [OOLReader] belongs to. */
        override val file: OOLFile<V, OutOfLineValue.Fixed>
            get() = this@FixedOOLFile

        /** A [ByteBuffer] holding the currently active segment. */
        private val segmentBuffer = ByteBuffer.allocate(this@FixedOOLFile.segmentSizeBytes)

        /** The segment ID that is currently in [segmentBuffer]. */
        private var segmentId: SegmentId = -1L

        /**
         * Reads a [Value] for the provided [OutOfLineValue.Fixed].
         *
         * @param row [OutOfLineValue.Fixed] that specifies the row to read.
         * @return [Value]
         */
        @Synchronized
        override fun read(row: OutOfLineValue.Fixed): V = this@FixedOOLFile.lock.read {
            val segmentId = floorDiv(row.rowId, this@FixedOOLFile.entryPerSegment)
            if (segmentId != this.segmentId || segmentId == this@FixedOOLFile.appendSegmentId) {
                this@FixedOOLFile.channelForSegment(segmentId).use { channel ->
                    channel.read(this.segmentBuffer.clear(), 0)
                    this.segmentId = segmentId
                }
            }
            val index = (row.rowId - (segmentId * this@FixedOOLFile.entryPerSegment)).toInt()
            val start = index * this@FixedOOLFile.type.physicalSize
            return this@FixedOOLFile.serializer.fromBuffer(this.segmentBuffer.slice(start, this@FixedOOLFile.type.physicalSize))
        }
    }

    /**
     * A [RandomReader] for a [FixedOOLFile].
     *
     * The [RandomReader] reads entry-by entry to minimize the number of bytes processed and does neither pre-fetch nor cache.
     */
    inner class RandomReader: OOLReader<V, OutOfLineValue.Fixed> {
        override val file: OOLFile<V, OutOfLineValue.Fixed>
            get() = this@FixedOOLFile

        /** The [ByteBuffer] to use for reading. */
        private val entryBuffer = ByteBuffer.allocate(this@FixedOOLFile.type.physicalSize)

        /**
         * Reads a [Value] for the provided [OutOfLineValue.Fixed].
         *
         * @param row [OutOfLineValue.Fixed] that specifies the row to read.
         * @return [Value]
         */
        override fun read(row: OutOfLineValue.Fixed): V = this@FixedOOLFile.lock.read {
            val segmentId = floorDiv(row.rowId, this@FixedOOLFile.entryPerSegment)
            val position = (row.rowId - segmentId * this@FixedOOLFile.entryPerSegment) * this@FixedOOLFile.type.physicalSize
            this@FixedOOLFile.channelForSegment(segmentId).use { channel ->
                channel.read(this.entryBuffer.clear(), position)
            }
            return this@FixedOOLFile.serializer.fromBuffer(this.entryBuffer.flip())
        }
    }

    /**
     * A [Writer] for a [FixedOOLFile].
     */
    inner class Writer: org.vitrivr.cottontail.storage.ool.interfaces.OOLWriter<V, OutOfLineValue.Fixed> {
        override val file: OOLFile<V, OutOfLineValue.Fixed>
            get() = this@FixedOOLFile

        /** The [ByteBuffer] to use for writing. */
        private val writeBuffer = ByteBuffer.allocate(this@FixedOOLFile.segmentSizeBytes)

        /** Internal row ID counter. */
        var rowId: Long
            private set

        init {
            this@FixedOOLFile.channelForSegment(this@FixedOOLFile.appendSegmentId).use { channel -> channel.read(this.writeBuffer) }
            val segmentIndex = floorDiv(this.writeBuffer.position(), this@FixedOOLFile.entryPerSegment)
            this.rowId = this@FixedOOLFile.appendSegmentId * this@FixedOOLFile.entryPerSegment + floorDiv(segmentIndex, this@FixedOOLFile.type.physicalSize)
        }

        /**
         * Appends a [Value] to this [FixedOOLFile].
         *
         * @param value The [Value] [V] to append.
         * @return [StoredValue] for the appended entry.
         */
        override fun append(value: V): OutOfLineValue.Fixed = this@FixedOOLFile.lock.write {
            val serialized = this@FixedOOLFile.serializer.toBuffer(value)
            if (serialized.capacity() > this.writeBuffer.remaining()) {
                this.flush()
            }
            this.writeBuffer.put(serialized)
            return OutOfLineValue.Fixed(this.rowId++)
        }

        /**
         * Flushes the data written through this [Writer] to the underlying [OOLFile].
         */
        override fun flush() = this@FixedOOLFile.lock.write {
            val segmentId = this@FixedOOLFile.appendSegmentId
            this@FixedOOLFile.channelForSegment(segmentId).use { channel ->
                val expected = this.writeBuffer.position()
                check(channel.write(this.writeBuffer.flip(), 0) == expected) { "Segment file truncated." }
                this.writeBuffer.limit(this.writeBuffer.capacity())
                if (!this.writeBuffer.hasRemaining()) {
                    this@FixedOOLFile.appendSegmentId += 1
                    this.writeBuffer.clear()
                }
                channel.force(true)
           }
        }
    }
}