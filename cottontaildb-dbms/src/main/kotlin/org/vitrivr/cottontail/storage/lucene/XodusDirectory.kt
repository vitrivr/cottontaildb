package org.vitrivr.cottontail.storage.lucene

import jetbrains.exodus.env.ContextualEnvironment
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.vfs.File
import jetbrains.exodus.vfs.VfsInputStream
import jetbrains.exodus.vfs.VirtualFileSystem
import org.apache.lucene.index.IndexFileNames
import org.apache.lucene.store.*
import org.vitrivr.cottontail.storage.lucene.XodusDirectory.SlicedIndexInput
import java.io.FileNotFoundException
import java.io.OutputStream
import java.lang.Integer.max
import java.lang.Long.min
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32

/**
 * A [Directory] class that uses the Xodus [VirtualFileSystem].
 *
 * Inspired by the original implementation in [1] but usable without a [ContextualEnvironment]. Instead,
 * this implementation is explicitly aware of the [Transaction] that is being used.
 *
 * @see [Directory]
 *
 * Links:
 * [1] https://github.com/JetBrains/xodus/tree/master/lucene-directory/src/main/kotlin/jetbrains/exodus/lucene
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class XodusDirectory(private val vfs: VirtualFileSystem, private val name: String, private val txn: Transaction, private val directoryConfig: XodusDirectoryConfig = XodusDirectoryConfig()): Directory() {

    init {
        require(!this.name.contains('/')) { "Name of the XodusDirectory must not contain a path separator." }
    }

    /** Internal counter used to create ticks. */
    private val ticks = AtomicLong(System.currentTimeMillis())

    /**
     * Lists all files in this [XodusDirectory]
     *
     * @return List of
     */
    override fun listAll(): Array<String> {
        val fqn = this.resolve("")
        return this@XodusDirectory.vfs.getFiles(this.txn).filter {
            it.path.startsWith(fqn)
        }.map {
            it.path.substring(fqn.length, it.path.length)
        }.toTypedArray()
    }

    /**
     * Deletes the file with the given file name.
     *
     * @param name The name of the file that should be deleted.
     */
    override fun deleteFile(name: String) {
        this.vfs.deleteFile(this.txn, this.resolve(name))
    }

    /**
     * Renames the file with the given filename.
     *
     * @param source The name of the file to rename.
     * @param dest The new filename.
     */
    override fun rename(source: String, dest: String) {
        val file = this.openFileOrThrow(source, false)
        this.vfs.renameFile(this.txn, file, this.resolve(dest))
    }

    /**
     * Obtains the size for the file with the given name.
     *
     * @param name Name of the file to lookup the name for.
     * @return The length of the file in bytes.
     */
    override fun fileLength(name: String): Long {
        val file = this.openFileOrThrow(name, false)
        return this@XodusDirectory.vfs.getFileLength(this.txn, file)
    }

    /**
     * Creates and returns a new [IndexOutput] for the given file in this [XodusDirectory].
     *
     * @param name The name of the file to create the [XodusDirectory.IndexOutput] for.
     * @param context The [IOContext].
     * @return The [IndexOutput].
     */
    override fun createOutput(name: String, context: IOContext): IndexOutput = IndexOutput(this.openFileOrThrow(name, true))

    /**
     * Creates and returns a new [IndexOutput] for the given temporary file in this [XodusDirectory].
     *
     * @param prefix The prefix of the temporary file to create the [XodusDirectory.IndexOutput] for.
     * @param suffix The suffix of the temporary file to create the [XodusDirectory.IndexOutput] for.
     * @param context The [IOContext].
     * @return The [IndexOutput].
     */
    override fun createTempOutput(prefix: String, suffix: String, context: IOContext): IndexOutput {
        val filename = IndexFileNames.segmentFileName(prefix, suffix + '_'.toString() + this.ticks.getAndIncrement(), "tmp")
        return createOutput(filename, context)
    }

    /**
     * Creates and returns a new [IndexInput] for the given file in this [XodusDirectory].
     *
     * @param name The name of the file to create the [XodusDirectory.IndexOutput] for.
     * @param context The [IOContext].
     * @return The [IndexInput].
     */
    override fun openInput(name: String, context: IOContext): IndexInput {
        val bufferSize = when (context.context) {
            IOContext.Context.MERGE -> this.directoryConfig.inputMergeBufferSize
            else -> this.directoryConfig.inputBufferSize
        }
        return IndexInput(this.openFileOrThrow(name, false), bufferSize)
    }

    /**
     * Creates and returns a new [ChecksumIndexInput] for the given file in this [XodusDirectory].
     *
     * @param name The name of the file to create the [XodusDirectory.ChecksumIndexInput] for.
     * @param context The [IOContext].
     * @return The [ChecksumIndexInput].
     */
    override fun openChecksumInput(name: String, context: IOContext) = ChecksumIndexInput(openInput(name, context))

    /**
     * [XodusDirectory.obtainLock] has no influence.
     *
     * @param name The name of the file to lock.
     * @return [Lock]
     */
    override fun obtainLock(name: String): Lock = NoLockFactory.INSTANCE.obtainLock(this, this.resolve(name))

    /**
     * Pending deletions always returns an empty set.
     */
    override fun getPendingDeletions(): MutableSet<String> = mutableSetOf()

    /**
     * Force flushes the data in the [EnvironmentImpl].
     */
    override fun sync(names: Collection<String>) = syncMetaData()

    /**
     * Force flushes the data in the [EnvironmentImpl].
     */
    override fun syncMetaData() = (this.vfs.environment as EnvironmentImpl).flushAndSync()


    override fun close() {
        /* No op. */
    }

    /**
     * Opens the file with the given name and throws an exception upon failure.
     *
     * @param name The name of the file to open.
     * @param create If true, file will be created if missing.
     * @return [File]
     */
    private fun openFileOrThrow(name: String, create: Boolean)
        = this.vfs.openFile(this@XodusDirectory.txn, this.resolve(name), create) ?: throw FileNotFoundException(name)

    /**
     * Resolves the given [String] file name given the location of this [XodusDirectory].
     *
     * @return Fully qualified name for the given filename.
     */
    private fun resolve(name: String) = "lucene/${this.name}/$name"

    /**
     * A [IndexInput] implementation for this [XodusDirectory].
     */
    open inner class IndexInput constructor(val file: File, bufferSize: Int):  BufferedIndexInput("XodusDirectory.IndexInput[$name]", bufferSize) {

        /** The current position of this [IndexOutput]. */
        private var currentPosition = 0L

        /** The [VfsInputStream] instance used by this [IndexInput]*/
        private var input: VfsInputStream = this@XodusDirectory.vfs.readFile(this@XodusDirectory.txn, this.file, this.currentPosition)

        /**
         * Clones and returns this [IndexInput].
         *
         * @return The cloned [IndexInput]
         */
        override fun clone(): IndexInput  {
            val clone = IndexInput(this.file, this.bufferSize)
            clone.seek(this.filePointer)
            return clone
        }

        /**
         * Returns the length of the file accessed by this [IndexInput].
         *
         * @return Length of the accessed file in bytes.
         */
        override fun length(): Long = this@XodusDirectory.vfs.getFileLength(this@XodusDirectory.txn, this.file)

        /**
         * Reads bytes into the given [ByteBuffer].
         *
         * @param b The [ByteBuffer] to read into.
         */
        override fun readInternal(b: ByteBuffer) {
            require(b.hasArray()) { "IndexInput.readInternal(ByteBuffer) expects a buffer with accessible array"}
            val offset = b.position()
            val read = this.input.read(b.array(), offset, b.limit() - offset)
            b.position(offset + read)
            this.currentPosition += read
            return
        }

        /**
         * Seeks to the given position.
         *
         * @param pos The position to seek to.
         */
        override fun seekInternal(pos: Long) {
            if (pos != this.currentPosition) {
                if (pos > this.currentPosition) {
                    val clusteringStrategy = this@XodusDirectory.vfs.config.clusteringStrategy
                    val bytesToSkip = pos - this.currentPosition
                    val clusterSize = clusteringStrategy.firstClusterSize
                    if ((!clusteringStrategy.isLinear || this.currentPosition % clusterSize + bytesToSkip < clusterSize) // or we are within single cluster
                        && this.input.skip(bytesToSkip) == bytesToSkip) {
                        this.currentPosition = pos
                        return
                    }
                }
                this.currentPosition = pos
            }
        }

        /**
         * Creates a sliced version of this [IndexInput].
         *
         * @param sliceDescription Descriptor of the [SlicedIndexInput]
         * @param offset Offset into the file of this [IndexInput].
         * @param length Length of the [SlicedIndexInput].
         * @return [SlicedIndexInput]
         */
        override fun slice(sliceDescription: String, offset: Long, length: Long) = SlicedIndexInput(this, offset, length)

        /**
         * Closes this [IndexInput].
         */
        override fun close() {
            this.input.close()
        }
    }

    /**
     * A sliced [IndexInput] implementation for this [XodusDirectory].
     */
    inner class SlicedIndexInput(val base: IndexInput, private val offset: Long, private val length: Long): IndexInput(base.file, max(min(base.bufferSize.toLong(), length).toInt(), MIN_BUFFER_SIZE)) {

        /** The length of a [SlicedIndexInput] is fixed. */
        override fun length() = this.length

        /**
         * Clones this [SlicedIndexInput].
         *
         * @return Clone of this [SlicedIndexInput]
         */
        override fun clone(): XodusDirectory.SlicedIndexInput {
            val clone = SlicedIndexInput(this.base, this.offset, this.length)
            clone.seek(this.filePointer)
            return clone
        }

        /**
         * Seeks to the given position.
         *
         * @param pos The position to seek to.
         */
        override fun seekInternal(pos: Long) = super.seekInternal(pos + this.offset)

        /**
         * Creates a sliced version of this [SlicedIndexInput].
         *
         * @param sliceDescription Descriptor of the [SlicedIndexInput]
         * @param offset Offset into the file of this [IndexInput].
         * @param length Length of the [SlicedIndexInput].
         * @return [SlicedIndexInput]
         */
        override fun slice(sliceDescription: String, offset: Long, length: Long)
            = this.base.slice(sliceDescription, this.offset + offset, length)
    }

    /**
     * A [IndexOutput] implementation for this [XodusDirectory].
     */
    inner class IndexOutput(val file: File): org.apache.lucene.store.IndexOutput("XodusDirectory.IndexOutput[${file.path}]", file.path.split('/').last()) {

        /** The [OutputStream] used by this [IndexOutput]. */
        private val output: OutputStream = this@XodusDirectory.vfs.writeFile(this@XodusDirectory.txn, this.file)

        /** The current position of this [IndexOutput]. */
        private var currentPosition = 0L

        /** [BufferedChecksum] of a [CRC32] checksum. */
        private val crc = BufferedChecksum(CRC32())

        /**
         * Returns the current  checksum value.
         *
         * @return Checksum value for this [IndexOutput].
         */
        override fun getChecksum() = this.crc.value

        /**
         * Returns the current position within the file.
         *
         * @return Checksum value for this [IndexOutput].
         */
        override fun getFilePointer() = this.currentPosition

        /**
         * Writes a single byte to this [XodusDirectory.IndexOutput].
         *
         * @param b The [Byte] to write.
         */
        override fun writeByte(b: Byte) {
            this.output.write(b.toInt())
            this.currentPosition += 1
            this.crc.update(b.toInt())
        }

        /**
         * Writes the content of a [ByteArray] to this [XodusDirectory.IndexOutput].
         *
         * @param b The [ByteArray] to write data from.
         * @param offset The offset into the source [ByteArray].
         * @param length The number of bytes to write.
         */
        override fun writeBytes(b: ByteArray, offset: Int, length: Int) {
            if (length == 0) return
            this.output.write(b, offset, length)
            this.currentPosition += length
            this.crc.update(b, offset, length)
        }

        /**
         *  Closes this [IndexOutput] and the associated [OutputStream].
         */
        override fun close() {
            this.output.close()
        }
    }

    /**
     * A [BufferedChecksumIndexInput] implementation that simply wraps an [IndexInput].
     */
    inner class ChecksumIndexInput(private val wrapped: IndexInput) : BufferedChecksumIndexInput(wrapped) {
        override fun skipBytes(numBytes: Long) {
            this.wrapped.seek( this.wrapped.filePointer + numBytes)
        }
    }
}