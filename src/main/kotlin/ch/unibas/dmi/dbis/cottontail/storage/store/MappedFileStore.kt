package ch.unibas.dmi.dbis.cottontail.storage.store

import ch.unibas.dmi.dbis.cottontail.model.exceptions.StoreException
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.roundUp
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.shift

import org.mapdb.DBException

import java.io.IOException
import java.nio.*
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.StandardOpenOption

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import kotlin.concurrent.withLock
import kotlin.math.min

/**
 * A [PersistentStore] backed by a [FileChannel] and a series of [MappedByteBuffer]s for data access. The store can handle
 * files of any size between 0 and [Long.MAX_VALUE] bytes. All data is written through the [FileChannel], however, read
 * access is facilitated by the [MappedByteBuffer].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class MappedFileStore(val path: Path, val readOnly: Boolean, val forceUnmap: Boolean = true, val lockTimeout: Long = 5000) : PersistentStore {

    companion object {
        /** Byte value written to the EOF of a grown [MappedFileStore] file. */
        const val EOF_BYTE = Byte.MIN_VALUE
    }

    /**The [FileChannel] used for data access. */
    private val fileChannel = if (this.readOnly) {
        FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.SPARSE)
    } else {
        FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SYNC, StandardOpenOption.SPARSE, StandardOpenOption.CREATE)
    }

    /** Acquires a [FileLock] for file underlying this [FileChannel]. */
    private val fileLock = this.acquireFileLock(this.lockTimeout)

    /** Internal lock used to mediate access to changes to the slicing. */
    private val sliceLock = ReentrantLock()

    /** The size of this [MappedFileStore] in bytes. */
    override val size: Long
        get() = this.fileChannel.size()

    /** A flag indicating whether or not this [MappedFileStore] was closed. */
    private var closed: AtomicBoolean = AtomicBoolean(false)

    /** The number of bits used to address the slice. */
    @Volatile
    private var sliceShift = 20

    /** The size of an individual slice (in bytes). */
    @Volatile
    private var sliceSize = 1L shl this.sliceShift

    /** Mask that can be used to identify the the slice part of the offset. */
    @Volatile
    private var sliceSizeModMask = this.sliceSize - 1L

    /** Internal list of [ByteBuffer] slices for data access. Gets initialized upon creation of this [MappedFileStore]. */
    @Volatile
    private var slices = emptyArray<MappedByteBuffer>()

    override fun getDouble(offset: Long): Double = this.sliceForOffset(offset).getDouble((offset and this.sliceSizeModMask).toInt())
    override fun putDouble(offset: Long, value: Double) {
        require(offset < this.size) { "Cannot write beyond size of MappedFileStore. Please call grow() first." }
        this.fileChannel.write(ByteBuffer.allocateDirect(8).putDouble(value).rewind(), offset)
    }

    override fun getFloat(offset: Long): Float = this.sliceForOffset(offset).getFloat((offset and this.sliceSizeModMask).toInt())
    override fun putFloat(offset: Long, value: Float) {
        require(offset < this.size) { "Cannot write beyond size of MappedFileStore. Please call grow() first." }
        this.fileChannel.write(ByteBuffer.allocateDirect(4).putFloat(value).rewind(), offset)
    }

    override fun getLong(offset: Long): Long = this.sliceForOffset(offset).getLong((offset and this.sliceSizeModMask).toInt())
    override fun putLong(offset: Long, value: Long) {
        require(offset < this.size) { "Cannot write beyond size of MappedFileStore. Please call grow() first." }
        this.fileChannel.write(ByteBuffer.allocateDirect(Long.SIZE_BYTES).putLong(value).rewind(), offset)
    }

    override fun getInt(offset: Long): Int = this.sliceForOffset(offset).getInt((offset and this.sliceSizeModMask).toInt())
    override fun putInt(offset: Long, value: Int) {
        require(offset < this.size) { "Cannot write beyond size of MappedFileStore. Please call grow() first." }
        this.fileChannel.write(ByteBuffer.allocateDirect(Int.SIZE_BYTES).putInt(value).rewind(), offset)
    }

    override fun getShort(offset: Long): Short = this.sliceForOffset(offset).getShort((offset and this.sliceSizeModMask).toInt())
    override fun putShort(offset: Long, value: Short) {
        require(offset < this.size) { "Cannot write beyond size of MappedFileStore. Please call grow() first." }
        this.fileChannel.write(ByteBuffer.allocateDirect(Short.SIZE_BYTES).putShort(value).rewind(), offset)
    }

    override fun getChar(offset: Long): Char = this.sliceForOffset(offset).getChar((offset and this.sliceSizeModMask).toInt())
    override fun putChar(offset: Long, value: Char) {
        require(offset < this.size) { "Cannot write beyond size of MappedFileStore. Please call grow() first." }
        this.fileChannel.write(ByteBuffer.allocateDirect(Char.SIZE_BYTES).putChar(value).rewind(), offset)
    }

    override fun getByte(offset: Long): Byte = this.sliceForOffset(offset).get((offset and this.sliceSizeModMask).toInt())
    override fun putByte(offset: Long, value: Byte) {
        require(offset < this.size) { "Cannot write beyond size of MappedFileStore. Please call grow() first." }
        this.fileChannel.write(ByteBuffer.allocateDirect(Byte.SIZE_BYTES).put(value).rewind(), offset)
    }

    override fun getData(offset: Long, dst: ByteArray, dstOffset: Int, dstLength: Int): ByteArray {
        this.sliceForOffset(offset).duplicate().position((offset and this.sliceSizeModMask).toInt()).get(dst, dstOffset, dstLength)
        return dst
    }

    override fun putData(offset: Long, src: ByteArray, srcOffset: Int, srcLength: Int) {
        require(offset < this.size) { "Cannot write beyond size of MappedFileStore. Please call grow() first." }
        this.fileChannel.write(ByteBuffer.wrap(src, srcOffset,srcLength), offset)
    }

    /**
     * Forces all the changes to this [MappedFileStore] to be written to disk.
     *
     * @return False, if this [MappedFileStore] is read only.
     */
    override fun force(): Boolean {
        if (this.readOnly)
            return false
        this.fileChannel.force(true)
        return true
    }

    /**
     * Tries to load the data of all the slices into physical memory.
     *
     * <p> As the [MappedByteBuffer.load] method, this method makes a best effort to ensure
     * that, when it returns, this [MappedFileStore]'s content is resident in physical memory.
     * Invoking this method may cause some number of page faults and I/O operations to
     * occur. </p>
     *
     * @return
     */
    override fun load() =this.slices.forEach { it.load() }

    /**
     * Clears the given range and writes it with all zeros.
     *
     * @param range The offset to clear.
     */
    override fun clear(range: LongRange) {
        val data = ByteBuffer.allocateDirect(8_000_000)
        val toWrite = (range.last-range.first)
        var written = 0L
        while (written < toWrite) {
            val bytesToWrite = min(data.capacity().toLong(), (toWrite-written + 1L))
            this.fileChannel.write(data.rewind().limit(bytesToWrite.toInt()), written)
            written += bytesToWrite
        }
    }

    /**
     * Truncates this [MappedFileStore] to the given size. May cause the memory mapping of the file to be adjusted.
     *
     * @param newSize The size to which to truncate this [MappedFileStore].
     */
    override fun truncate(newSize: Long) = this.sliceLock.withLock {
        /* Check if growth is required at all. */
        if (newSize > this.size) {
            return
        }

        /* Check if a complete re-mapping of the file is required after truncating the file. */
        val remapRequired = if (newSize > Integer.MAX_VALUE) {
            30
        } else {
            newSize.toInt().shift()
        } != this.sliceShift


        try {
            /* Truncate the file. */
            this.fileChannel.truncate(newSize)

            /* Either re-map or adjust existing mapping. */
            if (remapRequired) {
                this.remap()
                return
            } else {
                /* Determine size of slice array. */
                val slicesOld = this.slices.size
                val slicesNew: Int = (Math.floorDiv(newSize, this.sliceSize).toInt()) + 1

                /* Unmap existing slices. */
                if (this.forceUnmap) {
                    for (i in slicesNew-1 until slicesOld) {
                        CleanerUtility.forceUnmap(this.slices[i])
                    }
                }
                this.slices = this.slices.copyOfRange(0, slicesNew)

                /* Re-map the last slice. */
                val lastIndex = this.slices.lastIndex
                val chunkSize = if (this.sliceSize * lastIndex + this.sliceSize > newSize) {
                    this.sliceSize * lastIndex + this.sliceSize - newSize
                } else {
                    this.sliceSize
                }
                this.slices[lastIndex] = this.fileChannel.map(FileChannel.MapMode.READ_ONLY, this.sliceSize * lastIndex, chunkSize)
            }
        } catch (e: IOException) {
            throw DBException.VolumeIOError(e)
        }
    }

    /**
     * Grows this [MappedFileStore] so as to accommodate at least the given offset in bytes. Calling this method will pre-allocate the required space and update the memory mapping
     *
     * @param newSize The size to which to grow this [MappedFileStore].
     */
    override fun grow(newSize: Long) = this.sliceLock.withLock {
        /* Check if growth is required at all. */
        if (newSize < this.size) {
            return
        }

        /* Check if a complete re-mapping of the file is required after growing the file. */
        val remapRequired = if (newSize > Integer.MAX_VALUE) {
            30
        } else {
            newSize.toInt().shift()
        } != this.sliceShift

        /* Update the memory mappings. */
        try {
            /* Pre-clear file by appending a EOF byte. */
            this.fileChannel.write(ByteBuffer.allocate(1).put(EOF_BYTE).rewind(),  newSize-1)

            if (remapRequired) {
                this.remap()
                return
            } else {
                val slicesOld = this.slices.size
                val slicesNew: Int = (Math.floorDiv(newSize, this.sliceSize).toInt()) + 1
                if (slicesNew > slicesOld) {
                    val slices2 = (slicesOld until slicesNew).map {
                        val chunkSize = if (this.sliceSize * it + this.sliceSize > newSize) {
                            this.sliceSize * it + this.sliceSize - newSize
                        } else {
                            this.sliceSize
                        }
                        this.fileChannel.map(FileChannel.MapMode.READ_ONLY, this.sliceSize * it, chunkSize)
                    }.toTypedArray()
                    this.slices = (this.slices + slices2)
                }
            }
        } catch (e: IOException) {
            throw StoreException.StoreIOException(this, e)
        }
    }

    /**
     * Closes this [MappedFileStore].
     */
    override fun close() = this.sliceLock.withLock {
        if (!this.closed.compareAndSet(false, true))
            return

        try {
            /* Release lock and close FileChannel. */
            if (this.fileLock.isValid) {
                this.fileLock.release()
            }
            this.fileChannel.close()

            /* Unmap all the slices. */
            if (this.forceUnmap) {
                for (b in this.slices) {
                    CleanerUtility.forceUnmap(b)
                }
            }

            /* Remove reference to slices! */
            this.slices = emptyArray()
        } catch (e: IOException) {
            throw StoreException.StoreIOException(this, e)
        }
    }

    /**
     * Returns the slice for the given offset.
     *
     * @param offset The offset (in bytes) into the [MappedFileStore].
     */
    private fun sliceForOffset(offset: Long): ByteBuffer = this.sliceLock.withLock {
        val pos = offset.ushr(this.sliceShift).toInt()
        if (pos > this.slices.size) {
            throw StoreException.StoreEOFException(this, offset)
        }
        return this.slices[pos]
    }

    /**
     * Refreshes the memory mapping of this [MappedFileStore]. This can become necessary, if the file grows significantly.
     * The method determines the size of slice based on the file size. For large files, the slices become larger (up to 4G per slice).
     */
    private fun remap() = this.sliceLock.withLock {
        try {
            /* Force unmap the open slices (if existing). */
            if (this.forceUnmap) {
                for (slice in slices) {
                    CleanerUtility.forceUnmap(slice)
                }
            }

            /* Do some math regaring size of the slices. */
            val fileSize = this.fileChannel.size()
            this.sliceShift = if (fileSize >= Integer.MAX_VALUE) {
                30
            } else {
                fileSize.toInt().shift()
            }
            this.sliceSize = 1L shl this.sliceShift
            this.sliceSizeModMask = this.sliceSize - 1L

            /* Re-allocate the slices. */
            val chunks = fileSize.roundUp(this.sliceSize).ushr(this.sliceShift)
            this.slices = (0 until chunks).map {
                val chunkSize = if (this.sliceSize * it + this.sliceSize > fileSize) {
                    fileSize - this.sliceSize * it
                } else {
                    this.sliceSize
                }
                this.fileChannel.map(FileChannel.MapMode.READ_ONLY, sliceSize * it, chunkSize)
            }.toTypedArray()
        } catch (e: IOException) {
            throw StoreException.StoreIOException(this, e)
        }
    }

    /**
     * Tries to acquire a file lock of this [FileChannel] and returns it.
     *
     * @param timeout The amount of milliseconds to wait for lock.
     * @return lock The [FileLock] acquired.
     */
    private fun acquireFileLock(timeout: Long) : FileLock {
        val start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeout) {
            try {
                val lock = this.fileChannel.tryLock()
                if (lock != null) {
                    return lock
                } else {
                    Thread.sleep(250)
                }
            } catch (e: IOException) {
                throw StoreException.StoreIOException(this, e)
            }
        }
        throw StoreException.StoreLockException(this)
    }

    /**
     * Generates a string description of this [MappedFileStore].
     */
    override fun toString(): String = "MappedFileStore{file= ${this.path}, size = ${this.size}}"
}