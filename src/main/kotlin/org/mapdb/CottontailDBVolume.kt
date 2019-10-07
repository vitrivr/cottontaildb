package org.mapdb

import ch.unibas.dmi.dbis.cottontail.storage.store.FileChannelStore
import org.mapdb.volume.Volume
import org.mapdb.volume.VolumeFactory
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Path
import java.nio.file.Paths


/**
 * This is a re-implementation / copy of Map DB's [ByteBufferVol] class with minor modifications.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class CottontailDBVolume(val path: Path, val readonly: Boolean = false, val forceUnmap: Boolean = false, val initSize: Long = 2048L, val lockTimeout: Long = 5000) : Volume() {


    class CottontailDBVolumeFactory(val forceUnmap: Boolean) : VolumeFactory() {

        override fun makeVolume(file: String, readOnly: Boolean, fileLockWait: Long, sliceShift: Int, initSize: Long, fixedSize: Boolean): Volume {
            return factory(file, readOnly, fileLockWait, sliceShift, forceUnmap, initSize)
        }

        override fun exists(file: String?): Boolean {
            return File(file!!).exists()
        }

        override fun handlesReadonly(): Boolean {
            return true
        }

        private fun factory(file: String, readOnly: Boolean, fileLockWait: Long, sliceShift: Int, forceUnmap: Boolean, initSize: Long): Volume {
            val path = Paths.get(file)
            return CottontailDBVolume(path = path, readonly = readOnly, forceUnmap = forceUnmap, initSize = initSize, lockTimeout = fileLockWait)
        }
    }


    val store = FileChannelStore(path = this.path, readOnly = this.readonly, lockTimeout = this.lockTimeout)

    init {
        this.store.grow(initSize)
    }

    override fun isReadOnly(): Boolean = this.store.readOnly
    override fun getFile(): File = this.path.toFile()
    override fun length(): Long = this.store.size
    override fun clear(startOffset: Long, endOffset: Long) = this.store.clear(startOffset..endOffset)
    override fun truncate(size: Long) = this.store.truncate(size)
    override fun ensureAvailable(offset: Long) = this.store.grow(offset)

    override fun sync() {
        this.store.force()
    }

    override fun close() = this.store.close()

    override fun getFileLocked(): Boolean = true
    override fun sliceSize(): Int = 0
    override fun isSliced(): Boolean = false

    override fun getLong(offset: Long): Long = this.store.getLong(offset)
    override fun putLong(offset: Long, value: Long) = this.store.putLong(offset, value)

    override fun getInt(offset: Long): Int = this.store.getInt(offset)
    override fun putInt(offset: Long, value: Int) = this.store.putInt(offset, value)

    override fun getByte(offset: Long): Byte = this.store.getByte(offset)
    override fun putByte(offset: Long, value: Byte) = this.store.putByte(offset, value)

    override fun getData(offset: Long, bytes: ByteArray, bytesPos: Int, size: Int) {
        this.store.getData(offset, bytes, bytesPos, size)
    }

    override fun putData(offset: Long, src: ByteArray, srcPos: Int, srcSize: Int) = this.store.putData(offset, src, srcPos, srcSize)
    override fun putData(offset: Long, buf: ByteBuffer) = this.store.putData(offset, buf)

    override fun getDataInput(offset: Long, size: Int): DataInput2 = MappedFileStoreDataInput(this.store, offset)
}