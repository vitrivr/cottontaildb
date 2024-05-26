package org.vitrivr.cottontail.storage.ool

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.OutOfLineValue
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.storage.ool.interfaces.AccessPattern
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
import kotlin.math.min

/**
 * A [VariableOOLFile] supports variable-length records.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class VariableOOLFile<V: Value>(path: Path, type: Types<V>): AbstractOOLFile<V, OutOfLineValue.Variable>(path, type) {
    /** An internal counter of the highest segment ID. */
    override var appendSegmentId: Long = 0L

    init {
        val maxSegment =  Files.list(this.path).map { it.fileName.toString() }.filter { it.endsWith(".ool") }.map { it.substringBefore('.').toLong() }.max { o1, o2 -> o1.compareTo(o2) }.getOrElse { 0L }
        val maxSegmentPath = this.path.resolve("$maxSegment.ool")
        if (!Files.exists(maxSegmentPath) || Files.size(maxSegmentPath) < SEGMENT_SIZE) {
            this.appendSegmentId = maxSegment
        } else {
            this.appendSegmentId = maxSegment + 1
        }
    }

    /**
     * Provides a [OOLReader] for this [VariableOOLFile].
     *
     * @param pattern [AccessPattern] to use for reading.
     * @return [OOLReader]
     */
    override fun reader(pattern: AccessPattern): OOLReader<V, OutOfLineValue.Variable> = when(pattern) {
        AccessPattern.SEQUENTIAL -> SequentialReader()
        AccessPattern.RANDOM -> RandomReader()
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
    inner class SequentialReader: OOLReader<V, OutOfLineValue.Variable> {
        /** Reference to the [OOLFile] this [OOLReader] belongs to. */
        override val file: OOLFile<V, OutOfLineValue.Variable>
            get() = this@VariableOOLFile

        /** The [ByteBuffer] to use for reading. */
        private var entryBuffer = ByteBuffer.allocate(10)

        /** The [ByteBuffer] to use for reading. */
        private val segmentBuffer = ByteBuffer.allocate(SEGMENT_SIZE)

        /** The [LongRange] identifying the current segment. */
        private var segmentId = -1L
    
        /**
         * Reads a [Value] from the provided [StoredValue].
         *
         * @param row [OutOfLineValue.Variable] that specifies the row to read from.
         * @return [Value]
         */
        @Synchronized
        override fun read(row: OutOfLineValue.Variable): V = this@VariableOOLFile.lock.read {            /* Determine start and end segment. */
            /* Make sure entry buffer is large enough. */
            if (this.entryBuffer.capacity() < row.size) {
                this.entryBuffer = ByteBuffer.allocate(row.size)
            } else {
                this.entryBuffer.clear().limit(row.size)
            }

            /* Determine segments that should be read and read them in sequence. */
            val segmentsIds = floorDiv(row.position, SEGMENT_SIZE) .. floorDiv(row.position + row.size, SEGMENT_SIZE)
            for (segmentId in segmentsIds) {
                if (segmentId != this.segmentId || segmentId == this@VariableOOLFile.appendSegmentId) {
                    this@VariableOOLFile.channelForSegment(segmentId).use { channel ->
                        channel.read(this.segmentBuffer, 0)
                        this.segmentId = segmentId
                        this.segmentBuffer.flip()
                    }
                }
                if (segmentId == segmentsIds.first) {
                    val start = (row.position - segmentId * SEGMENT_SIZE).toInt()
                    this.entryBuffer.put(this.segmentBuffer.slice(start, min(this.entryBuffer.remaining(), this.segmentBuffer.capacity() - start)))
                } else {
                    this.entryBuffer.put(this.segmentBuffer.slice(0, min(this.entryBuffer.remaining(), this.segmentBuffer.capacity())))
                }
            }

            /* Parse values. */
            return this@VariableOOLFile.serializer.fromBuffer(this.entryBuffer.flip())
        }
    }
    
    /**
     * A [RandomReader] for a [FixedOOLFile].
     *
     * The [RandomReader] reads entry-by entry to minimize the number of bytes processed and does neither pre-fetch nor cache.
     */
    inner class RandomReader: OOLReader<V, OutOfLineValue.Variable> {
        /** Reference to the [OOLFile] this [OOLReader] belongs to. */
        override val file: OOLFile<V, OutOfLineValue.Variable>
            get() = this@VariableOOLFile
    
        /** The [ByteBuffer] to use for reading. */
        private var entryBuffer = ByteBuffer.allocate(10)

        /**
         * Reads a [Value] for the provided [OutOfLineValue.Variable].
         *
         * @param row [OutOfLineValue.Variable] that specifies the row to read.
         * @return [Value]
         */
        @Synchronized
        override fun read(row: OutOfLineValue.Variable): V = this@VariableOOLFile.lock.read {
            if (this.entryBuffer.capacity() < row.size) {
                this.entryBuffer = ByteBuffer.allocate(row.size)
            } else {
                this.entryBuffer.clear().limit(row.size)
            }

            /* Obtain segmentd IDs that should be read. */
            val segmentsIds = floorDiv(row.position, SEGMENT_SIZE) .. floorDiv(row.position + row.size, SEGMENT_SIZE)
            var remaining = row.size

            /* Read data from segments. */
            for (segmentId in segmentsIds) {
                this@VariableOOLFile.channelForSegment(segmentId).use { channel ->
                    remaining -= if (segmentId == segmentsIds.first) {
                        channel.read(this.entryBuffer, row.position - segmentId * SEGMENT_SIZE)
                    } else {
                        channel.read(this.entryBuffer, 0)
                    }
                }
            }

            return this@VariableOOLFile.serializer.fromBuffer(this.entryBuffer.flip())
        }
    }
    
    /**
     * A [Writer] for a [VariableOOLFile].
     */
    inner class Writer: org.vitrivr.cottontail.storage.ool.interfaces.OOLWriter<V, OutOfLineValue.Variable> {
        override val file: OOLFile<V, OutOfLineValue.Variable>
            get() = this@VariableOOLFile

        /** The [ByteBuffer] to use for writing. */
        private val writeBuffer = ByteBuffer.allocate(SEGMENT_SIZE)

        init {
            this@VariableOOLFile.channelForSegment(this@VariableOOLFile.appendSegmentId).use { channel ->
                channel.read(this.writeBuffer, 0)
            }
        }

        /**
         * Appends a [Value] to this [VariableOOLFile].
         *
         * @param value The [Value] [V] to append.
         * @return [OutOfLineValue.Variable] for the appended entry.
         */
        override fun append(value: V): OutOfLineValue.Variable = this@VariableOOLFile.lock.write {
            /* Determine start and end segment. */
            val serialized = this@VariableOOLFile.serializer.toBuffer(value)
            val position = this@VariableOOLFile.appendSegmentId * SEGMENT_SIZE + this.writeBuffer.position()
            val size = serialized.remaining()
            do {
                val limit = min(this.writeBuffer.remaining(), serialized.capacity() - serialized.position())
                this.writeBuffer.put(serialized.limit(serialized.position() + limit))
                if (!this.writeBuffer.hasRemaining()) {
                    this.flush()
                }
            } while (serialized.capacity() - serialized.position() != 0)
            return OutOfLineValue.Variable(position, size)
        }

        /**
         * Flushes the data written through this [Writer] to the underlying [OOLFile].
         */
        override fun flush() = this@VariableOOLFile.lock.write {
            this@VariableOOLFile.channelForSegment(this@VariableOOLFile.appendSegmentId).use { channel ->
                val expected = this.writeBuffer.position()
                check(channel.write(this.writeBuffer.slice(0, expected), 0) == expected) { "Segment file truncated." }
                if (!this.writeBuffer.hasRemaining()) {
                    this@VariableOOLFile.appendSegmentId += 1
                    this.writeBuffer.clear()
                }
                channel.force(true)
            }
        }
    }
}