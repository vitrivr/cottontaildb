package org.vitrivr.cottontail.storage.entries

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.dbms.entity.values.StoredValueRef
import org.vitrivr.cottontail.storage.entries.interfaces.DataFile
import org.vitrivr.cottontail.storage.entries.interfaces.DataFile.Companion.SEGMENT_SIZE
import org.vitrivr.cottontail.storage.entries.interfaces.Reader
import org.vitrivr.cottontail.storage.entries.interfaces.ReaderPattern
import java.lang.Math.floorDiv
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.min

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class VariableLengthFile<V: Value>(path: Path, name: String, type: Types<V>): AbstractDataFile<V, StoredValueRef.OutOfLine.Variable>(path, name, type) {
    /**
     * Provides a [Reader] for this [VariableLengthFile].
     *
     * @param pattern [ReaderPattern] to use for reading.
     * @return [Reader]
     */
    override fun reader(pattern: ReaderPattern): Reader<V, StoredValueRef.OutOfLine.Variable> = when(pattern) {
        ReaderPattern.SEQUENTIAL -> SequentialReader()
        ReaderPattern.RANDOM -> RandomReader()
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
    inner class SequentialReader: Reader<V, StoredValueRef.OutOfLine.Variable> {
        /** Reference to the [DataFile] this [Reader] belongs to. */
        override val file: DataFile<V, StoredValueRef.OutOfLine.Variable>
            get() = this@VariableLengthFile

        /** The [ByteBuffer] to use for reading. */
        private var entryBuffer = ByteBuffer.allocate(10)

        /** The [ByteBuffer] to use for reading. */
        private val segmentBuffer = ByteBuffer.allocate(SEGMENT_SIZE)

        /** The [LongRange] identifying the current segment. */
        private var segmentId = -1L
    
        /**
         * Reads a [Value] from the provided [StoredValue].
         *
         * @param row [StoredValueRef.OutOfLine.Variable] that specifies the row to read from.
         * @return [Value]
         */
        @Synchronized
        override fun read(row: StoredValueRef.OutOfLine.Variable): V = this@VariableLengthFile.lock.read {            /* Determine start and end segment. */
            /* Make sure entry buffer is large enough. */
            if (this.entryBuffer.capacity() < row.size) {
                this.entryBuffer = ByteBuffer.allocate(row.size)
            }
            this.entryBuffer.clear()

            /* Determine segments that should be read and read them in sequence. */
            val segmentsIds = floorDiv(row.position, SEGMENT_SIZE) .. floorDiv(row.position + row.size, SEGMENT_SIZE)
            var remaining = row.size
            for (segmentId in segmentsIds) {
                if (segmentId != this.segmentId || segmentId == this@VariableLengthFile.appendSegmentId.get()) {
                    this@VariableLengthFile.getChannel(segmentId).read(this.segmentBuffer.clear(), 0)
                    this.segmentId = segmentId
                    this.segmentBuffer.clear()
                }
                val read = min(remaining, this.segmentBuffer.remaining())
                if (segmentId == segmentsIds.first) {
                    val start = (row.position - segmentId * SEGMENT_SIZE).toInt()
                    this.entryBuffer.put(this.segmentBuffer.position(start).limit(start + read))
                } else {
                    this.entryBuffer.put(this.segmentBuffer.position(0).limit(read))
                }
                remaining = read
            }

            /* Parse values. */
            return this@VariableLengthFile.serializer.fromBuffer(this.entryBuffer.flip())
        }
    }
    
    /**
     * A [RandomReader] for a [FixedLengthFile].
     *
     * The [RandomReader] reads entry-by entry to minimize the number of bytes processed and does neither pre-fetch nor cache.
     */
    inner class RandomReader: Reader<V, StoredValueRef.OutOfLine.Variable> {
        /** Reference to the [DataFile] this [Reader] belongs to. */
        override val file: DataFile<V, StoredValueRef.OutOfLine.Variable>
            get() = this@VariableLengthFile
    
        /** The [ByteBuffer] to use for reading. */
        private var entryBuffer = ByteBuffer.allocate(10)

        /**
         * Reads a [Value] for the provided [StoredValueRef.OutOfLine.Variable].
         *
         * @param row [StoredValueRef.OutOfLine.Variable] that specifies the row to read.
         * @return [Value]
         */
        @Synchronized
        override fun read(row: StoredValueRef.OutOfLine.Variable): V = this@VariableLengthFile.lock.read {
            if (this.entryBuffer.capacity() < row.size) {
                this.entryBuffer = ByteBuffer.allocate(row.size)
            }
            val segmentId = floorDiv(row.position, SEGMENT_SIZE)
            val readPosition = row.position - segmentId * SEGMENT_SIZE
            val channel = this@VariableLengthFile.getChannel(segmentId)
            channel.read(this.entryBuffer.position(0).limit(row.size), readPosition)
            return this@VariableLengthFile.serializer.fromBuffer(this.entryBuffer.flip())
        }
    }
    
    /**
     * A [Writer] for a [VariableLengthFile].
     */
    inner class Writer: org.vitrivr.cottontail.storage.entries.interfaces.Writer<V, StoredValueRef.OutOfLine.Variable> {
        override val file: DataFile<V, StoredValueRef.OutOfLine.Variable>
            get() = this@VariableLengthFile

        /** The [ByteBuffer] to use for writing. */
        private val writeBuffer = ByteBuffer.allocate(SEGMENT_SIZE)

        /** Number of bytes of the current segment that have been flushed. */
        private var segmentFlushed: Int

        init {
            this@VariableLengthFile.getChannel(this@VariableLengthFile.appendSegmentId.get()).read(this.writeBuffer.clear())
            this.segmentFlushed = this.writeBuffer.position()
        }

        /**
         * Appends a [Value] to this [VariableLengthFile].
         *
         * @param value The [Value] [V] to append.
         * @return [StoredValueRef.OutOfLine.Variable] for the appended entry.
         */
        override fun append(value: V): StoredValueRef.OutOfLine.Variable = this@VariableLengthFile.lock.write {
            /* Determine start and end segment. */
            val serialized = this@VariableLengthFile.serializer.toBuffer(value).clear()
            val size = serialized.remaining()
            val position = this@VariableLengthFile.appendSegmentId.get() * SEGMENT_SIZE + this.writeBuffer.position()
            if (serialized.remaining() <= this.writeBuffer.remaining()) {
                this.writeBuffer.put(serialized)
            } else {
                this.writeBuffer.put(serialized.limit(this.writeBuffer.remaining()))
                do {
                    this.flush()
                    this.writeBuffer.put(serialized.limit(min(serialized.remaining(),this.writeBuffer.remaining())))
                    serialized.limit(serialized.capacity())
                } while (serialized.hasRemaining())
            }
            return StoredValueRef.OutOfLine.Variable(position, size)
        }

        /**
         * Flushes the data written through this [Writer] to the underlying [DataFile].
         */
        override fun flush() = this@VariableLengthFile.lock.write {
            val segmentId = this@VariableLengthFile.appendSegmentId.get()
            val channel = this@VariableLengthFile.getChannel(segmentId)
            if (this.writeBuffer.hasRemaining()) {
                val position = this.writeBuffer.position()
                this.writeBuffer.position(this.segmentFlushed).limit(position)
                this.segmentFlushed += channel.write(this.writeBuffer)
                this.writeBuffer.limit(this.writeBuffer.capacity()).position(position)
            } else {
                channel.write(this.writeBuffer.flip())
                this.writeBuffer.clear()

                /* Update append segment ID. */
                this@VariableLengthFile.appendSegmentId.getAndIncrement()

                /* Reset counters. */
                this.segmentFlushed = 0
            }
            channel.force(true)
        }
    }
}