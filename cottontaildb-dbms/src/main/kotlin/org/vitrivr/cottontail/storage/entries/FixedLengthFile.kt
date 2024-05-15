package org.vitrivr.cottontail.storage.entries

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.dbms.entity.values.StoredValueRef
import org.vitrivr.cottontail.storage.entries.interfaces.DataFile
import org.vitrivr.cottontail.storage.entries.interfaces.DataFile.Companion.SEGMENT_SIZE
import org.vitrivr.cottontail.storage.entries.interfaces.Reader
import org.vitrivr.cottontail.storage.entries.interfaces.ReaderPattern
import org.vitrivr.cottontail.storage.entries.interfaces.ReaderPattern.RANDOM
import org.vitrivr.cottontail.storage.entries.interfaces.ReaderPattern.SEQUENTIAL
import java.lang.Math.floorDiv
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * A [FixedLengthFile] used as a simple, append-only file for [Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FixedLengthFile<V: Value>(path: Path, name: String, type: Types<V>): AbstractDataFile<V, StoredValueRef.OutOfLine.Fixed>(path, name, type) {
    /** The size of a segment in bytes. */
    private val entryPerSegment: Int = (floorDiv(SEGMENT_SIZE, this.type.physicalSize) + 1)

    /** The size of a segment in bytes. */
    private val segmentSizeBytes: Int = this.entryPerSegment  * this.type.physicalSize

    /**
     * Provides a [Reader] for this [DataFile].
     *
     * @param pattern [ReaderPattern] to use for reading.
     * @return [Reader]
     */
    override fun reader(pattern: ReaderPattern) = when(pattern) {
        SEQUENTIAL -> SequentialReader()
        RANDOM -> RandomReader()
    }

    /**
     * Creates a new [Writer] for this [AbstractDataFile].
     *
     * @return New [Writer].
     */
    override fun newWriter() = Writer()

    /**
     * A [SequentialReader] for a [FixedLengthFile].
     *
     * The [SequentialReader] (pre-)fetches entries in larger segments and caches them in memory to minimize latency.
     */
    inner class SequentialReader: Reader<V, StoredValueRef.OutOfLine.Fixed> {
        /** Reference to the [DataFile] this [Reader] belongs to. */
        override val file: DataFile<V, StoredValueRef.OutOfLine.Fixed>
            get() = this@FixedLengthFile

        /** */
        private val buffer = ByteBuffer.allocate(this@FixedLengthFile.segmentSizeBytes)

        /** */
        private var segmentId: Long = -1L

        /**
         * Reads a [Value] for the provided [StoredValueRef.OutOfLine.Fixed].
         *
         * @param row [StoredValueRef.OutOfLine.Fixed] that specifies the row to read.
         * @return [Value]
         */
        @Synchronized
        override fun read(row: StoredValueRef.OutOfLine.Fixed): V = this@FixedLengthFile.lock.read {
            val segmentId = floorDiv(row.rowId, this@FixedLengthFile.entryPerSegment)
            val channel = this@FixedLengthFile.getChannel(segmentId)
            val index = (row.rowId - (segmentId * this@FixedLengthFile.entryPerSegment)).toInt()
            if (segmentId != this.segmentId || segmentId == this@FixedLengthFile.appendSegmentId.get()) {
                channel.read(this.buffer.clear(), segmentId * this@FixedLengthFile.segmentSizeBytes)
                this.segmentId = segmentId
            }
            val start = index * this@FixedLengthFile.type.physicalSize
            return this@FixedLengthFile.serializer.fromBuffer(this.buffer.slice(start, this@FixedLengthFile.type.physicalSize))
        }
    }

    /**
     * A [RandomReader] for a [FixedLengthFile].
     *
     * The [RandomReader] reads entry-by entry to minimize the number of bytes processed and does neither pre-fetch nor cache.
     */
    inner class RandomReader: Reader<V, StoredValueRef.OutOfLine.Fixed> {
        override val file: DataFile<V, StoredValueRef.OutOfLine.Fixed>
            get() = this@FixedLengthFile

        /** The [ByteBuffer] to use for reading. */
        private val entryBuffer = ByteBuffer.allocate(this@FixedLengthFile.type.physicalSize)

        /**
         * Reads a [Value] for the provided [StoredValueRef.OutOfLine.Fixed].
         *
         * @param row [StoredValueRef.OutOfLine.Fixed] that specifies the row to read.
         * @return [Value]
         */
        override fun read(row: StoredValueRef.OutOfLine.Fixed): V = this@FixedLengthFile.lock.read {
            val segmentId = floorDiv(row.rowId, this@FixedLengthFile.entryPerSegment)
            val position = (row.rowId - segmentId * this@FixedLengthFile.entryPerSegment) * this@FixedLengthFile.type.physicalSize
            val channel = this@FixedLengthFile.getChannel(segmentId)
            channel.read(this.entryBuffer.clear(), position)
            return this@FixedLengthFile.serializer.fromBuffer(this.entryBuffer.flip())
        }
    }

    /**
     * A [Writer] for a [FixedLengthFile].
     */
    inner class Writer: org.vitrivr.cottontail.storage.entries.interfaces.Writer<V, StoredValueRef.OutOfLine.Fixed> {
        override val file: DataFile<V, StoredValueRef.OutOfLine.Fixed>
            get() = this@FixedLengthFile

        /** The [ByteBuffer] to use for writing. */
        private val writeBuffer = ByteBuffer.allocate(this@FixedLengthFile.segmentSizeBytes)

        /** Internal row ID counter. */
        private var rowId: Long

        /** Number of bytes of the current segment that have been flushed. */
        private var segmentFlushed: Int

        init {
           this@FixedLengthFile.getChannel(this@FixedLengthFile.appendSegmentId.get()).read(this.writeBuffer.clear())
            val segmentIndex = floorDiv(this.writeBuffer.position(), this@FixedLengthFile.entryPerSegment)
            this.rowId = this@FixedLengthFile.appendSegmentId.get() * this@FixedLengthFile.entryPerSegment + floorDiv(segmentIndex, this@FixedLengthFile.type.physicalSize)
            this.segmentFlushed = this.writeBuffer.position()
        }

        /**
         * Appends a [Value] to this [FixedLengthFile].
         *
         * @param value The [Value] [V] to append.
         * @return [StoredValue] for the appended entry.
         */
        override fun append(value: V): StoredValueRef.OutOfLine.Fixed = this@FixedLengthFile.lock.write {
            val serialized = this@FixedLengthFile.serializer.toBuffer(value)
            if (serialized.capacity() > this.writeBuffer.remaining()) {
                this.flush()
            }
            this.writeBuffer.put(serialized)
            return StoredValueRef.OutOfLine.Fixed(this.rowId++)
        }

        /**
         * Flushes the data written through this [Writer] to the underlying [DataFile].
         */
        override fun flush() = this@FixedLengthFile.lock.write {
            val segmentId = this@FixedLengthFile.appendSegmentId.get()
            val channel = this@FixedLengthFile.getChannel(segmentId)
            if (this.writeBuffer.hasRemaining()) {
                val position = this.writeBuffer.position()
                this.writeBuffer.position(this.segmentFlushed).limit(position)
                this.segmentFlushed += channel.write(this.writeBuffer)
                this.writeBuffer.limit(this.writeBuffer.capacity()).position(position)
            } else {
                channel.write(this.writeBuffer.flip())
                this.writeBuffer.clear()

                /* Update append segment ID. */
                this@FixedLengthFile.appendSegmentId.getAndIncrement()

                /* Reset counters. */
                this.segmentFlushed = 0
            }
            channel.force(true)
        }
    }
}