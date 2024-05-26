package org.vitrivr.cottontail.storage.ool

import it.unimi.dsi.fastutil.objects.Object2ObjectFunction
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.OutOfLineValue
import org.vitrivr.cottontail.serialization.buffer.ValueSerializer
import org.vitrivr.cottontail.storage.ool.interfaces.OOLFile
import org.vitrivr.cottontail.storage.ool.interfaces.OOLWriter
import java.lang.ref.WeakReference
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock

/** */
typealias SegmentId = Long

/**
 * An abstract [OOLFile] implementation that provides basic functionality.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractOOLFile<V: Value, D: OutOfLineValue>(final override val path: Path, final override val type: Types<V>): OOLFile<V, D> {
    companion object {
        /** Maximum number of pen [SharedFileChannel]s. */
        private const val MAX_CHANNELS = 128

        /** An internal [TreeMap] of [FileChannel]s used to access the underlying segment file. */
        private val OPEN_CHANNELS = Object2ObjectLinkedOpenHashMap<Path,SharedFileChannel>()
    }

    init {
        /* Create directories if necessary. */
        if (Files.notExists(this.path)) {
            Files.createDirectories(this.path)
        }
    }

    /** The [ValueSerializer] used to serialize and de-serialize entries. */
    @Suppress("UNCHECKED_CAST")
    protected val serializer = ValueSerializer.serializer(this.type) as ValueSerializer<V>

    /** An internal counter of the segment ID to append data to. */
    abstract var appendSegmentId: Long
        protected set

    /** A [ReentrantReadWriteLock] to mediate access to the [OOLFile]. */
    protected val lock = ReentrantReadWriteLock()

    /** A [WeakReference] to a common [OOLWriter] instance. */
    private var writer: WeakReference<OOLWriter<V, D>>? = null

    /**
     * Provides a [OOLWriter] for this [AbstractOOLFile].
     *
     * Method makes sure, that only a single [OOLWriter] is created per [AbstractOOLFile].
     *
     * @return [OOLWriter]
     */
    @Synchronized
    final override fun writer(): OOLWriter<V, D>{
        val w = this.writer?.get()
        if (w == null) {
            val new = this.newWriter()
            this.writer = WeakReference(new)
            return new
        } else {
            return w
        }
    }


    /**
     * Obtains a [SharedFileChannel] for the specified segment.
     *
     * @return [SharedFileChannel]
     */
    protected fun channelForSegment(segmentId: SegmentId): SharedFileChannel {
        val path = this.path.resolve("$segmentId.ool")
        val ret = OPEN_CHANNELS.computeIfAbsent(path, Object2ObjectFunction {
            SharedFileChannel(if (segmentId == this.appendSegmentId) {
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
            } else {
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ)
            })
        }).retain()

        /* Clean up the cache if it gets too large. */
        if (OPEN_CHANNELS.size >= MAX_CHANNELS) {
            OPEN_CHANNELS.object2ObjectEntrySet().removeIf {
                if (it.value.referenceCount == 0 && it.value != ret) {
                    it.value.close()
                    true
                } else {
                    false
                }
            }
        }

        /* Return SharedFileChannel. */
        return ret
    }

    /**
     * Creates a new [OOLWriter] for this [AbstractOOLFile].
     *
     * @return New [OOLWriter].
     */
    protected abstract fun newWriter(): OOLWriter<V, D>
}