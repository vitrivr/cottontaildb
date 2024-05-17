package org.vitrivr.cottontail.storage.ool

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectFunction
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.StoredValueRef
import org.vitrivr.cottontail.storage.ool.interfaces.OOLFile
import org.vitrivr.cottontail.storage.ool.interfaces.OOLWriter
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import java.lang.ref.WeakReference
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.jvm.optionals.getOrElse

/** */
typealias SegmentId = Long

/**
 * An abstract [OOLFile] implementation that provides basic functionality.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractOOLFile<V: Value, D: StoredValueRef>(final override val path: Path, final override val type: Types<V>): OOLFile<V, D> {
    companion object {
        /** Maximum number of pen [SharedFileChannel]s. */
        private const val MAX_CHANNELS = 128

        /** An internal [TreeMap] of [FileChannel]s used to access the underlying segment file. */
        private val OPEN_CHANNELS = Long2ObjectOpenHashMap<SharedFileChannel>()
    }

    init {
        /* Create directories if necessary. */
        if (Files.notExists(this.path)) {
            Files.createDirectories(this.path)
        }
    }

    /** The [ValueSerializer] used to serialize and de-serialize entries. */
    protected val serializer: ValueSerializer<V> = SerializerFactory.value(this.type)

    /** An internal counter of the highest segment ID. */
    protected var appendSegmentId: Long = Files.list(this.path).map {
        it.fileName
    }.filter {
        it.endsWith(".ool")
    }.map {
        it.toString().substringAfterLast('.').toLong()
    }.max {
        o1, o2 -> o1.compareTo(o2)
    }.getOrElse { 0L }

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
        val ret = OPEN_CHANNELS.computeIfAbsent(segmentId, Object2ObjectFunction {
            val path = this.path.resolve("$segmentId.ool")
            SharedFileChannel(if (segmentId == this.appendSegmentId) {
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
            } else {
                FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ)
            })
        }).retain()

        /* Clean up the cache if it gets too large. */
        if (OPEN_CHANNELS.size >= MAX_CHANNELS) {
            OPEN_CHANNELS.long2ObjectEntrySet().removeIf {
                it.value.referenceCount == 0 && it.value != ret
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