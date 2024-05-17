package org.vitrivr.cottontail.storage.ool

import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.*

/**
 * A [SharedFileChannel] is a wrapper around a [FileChannel] that allows for reference counting.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SharedFileChannel(private val wrapped: FileChannel): SeekableByteChannel by wrapped, GatheringByteChannel by wrapped, ScatteringByteChannel by wrapped, InterruptibleChannel by wrapped, Comparable<SharedFileChannel>{

    /** Internal reference counter. */
    @Volatile
    var referenceCount = 0
        private set

    /** Timestamp of the last use for this [SharedFileChannel]. */
    @Volatile
    var lastUsed: Long = System.currentTimeMillis()
        private set

    /**
     * Retains this [SharedFileChannel] by incrementing the reference counter.
     *
     * @return This [SharedFileChannel]
     */
    @Synchronized
    fun retain(): SharedFileChannel {
        this.lastUsed = System.currentTimeMillis()
        this.referenceCount += 1
        return this
    }
    override fun isOpen(): Boolean = this.wrapped.isOpen
    override fun read(dst: ByteBuffer): Int = this.wrapped.read(dst)
    override fun write(dst: ByteBuffer): Int = this.wrapped.write(dst)
    override fun read(dsts: Array<out ByteBuffer>, offset: Int, length: Int): Long = this.wrapped.read(dsts, offset, length)
    fun read(dst: ByteBuffer, position: Long): Int = this.wrapped.read(dst, position)
    fun write(src: ByteBuffer?, position: Long): Int = this.wrapped.write(src, position)
    fun force(metaData: Boolean) = this.wrapped.force(metaData)
    fun transferTo(position: Long, count: Long, target: WritableByteChannel?): Long = this.wrapped.transferTo(position, count, target)
    fun transferFrom(src: ReadableByteChannel?, position: Long, count: Long): Long = this.wrapped.transferFrom(src, position, count)
    fun map(mode: FileChannel.MapMode?, position: Long, size: Long): MappedByteBuffer = this.wrapped.map(mode, position, size)
    fun lock(position: Long, size: Long, shared: Boolean): FileLock = this.wrapped.lock(position, size, shared)
    fun tryLock(position: Long, size: Long, shared: Boolean): FileLock = this.wrapped.tryLock(position, size, shared)

    /**
     * Decrements the reference counter on this [SharedFileChannel].
     *
     * If the reference counter reaches 0, the underlying [FileChannel] is closed.
     */
    @Synchronized
    override fun close() {
        if (this.wrapped.isOpen && (--this.referenceCount) < 0) {
            this.wrapped.close()
        }
    }

    /**
     * Compares this [SharedFileChannel] to another [SharedFileChannel] based on their reference count and last used timestamp.
     *
     * @param other The other [SharedFileChannel] to compare to.
     */
    override fun compareTo(other: SharedFileChannel): Int {
        val ret = this.referenceCount.compareTo(other.referenceCount)
        return if (ret == 0) {
            this.lastUsed.compareTo(other.lastUsed)
        } else {
            ret
        }
    }
}