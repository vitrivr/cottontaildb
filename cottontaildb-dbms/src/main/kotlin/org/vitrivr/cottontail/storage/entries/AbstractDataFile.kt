package org.vitrivr.cottontail.storage.entries

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import com.github.benmanes.caffeine.cache.RemovalListener
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.StoredValueRef
import org.vitrivr.cottontail.storage.entries.interfaces.DataFile
import org.vitrivr.cottontail.storage.entries.interfaces.Writer
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.jvm.optionals.getOrElse

/** */
typealias SegmentId = Long

/**
 * An abstract [DataFile] implementation that provides basic functionality.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractDataFile<V: Value, D: StoredValueRef>(final override val path: Path, val name: String, final override val type: Types<V>): DataFile<V, D> {

    /** The [ValueSerializer] used to serialize and de-serialize entries. */
    protected val serializer: ValueSerializer<V> = SerializerFactory.value(this.type)

    /** A [ReentrantReadWriteLock] to mediate access to the [FileChannel]. */
    protected val lock = ReentrantReadWriteLock()

    /** An internal counter of the highest segment ID. */
    protected val appendSegmentId: AtomicLong = Files.list(this.path).map {
        it.fileName
    }.filter {
        it.startsWith("$name.") && it.endsWith(".ool")
    }.map {
        it.toString().substringAfterLast('.').toLong()
    }.max {
        o1, o2 -> o1.compareTo(o2)
    }.map { AtomicLong(0) }.getOrElse { AtomicLong(0L) }

    /**
     * An internal cache of [FileChannel]s used to access the underlying segment file.
     */
    private val channels: LoadingCache<SegmentId, FileChannel> = Caffeine.newBuilder().maximumSize(10).evictionListener(RemovalListener<SegmentId, FileChannel> {
        _, value, _ -> value?.close()
    }).build { key ->
        if (key == this.appendSegmentId.get()) {
            FileChannel.open(this.path.resolve("$name.$key.ool"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
        } else {
            FileChannel.open(this.path.resolve("$name.$key.ool"), StandardOpenOption.READ)
        }
    }


    /** A [WeakReference] to a common [Writer] instance. */
    private var writer: WeakReference<Writer<V, D>>? = null

    /**
     * Provides a [Writer] for this [AbstractDataFile].
     *
     * Method makes sure, that only a single [Writer] is created per [AbstractDataFile].
     *
     * @return [Writer]
     */
    @Synchronized
    final override fun writer(): Writer<V, D>{
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
     * Creates a new [Writer] for this [AbstractDataFile].
     *
     * @return New [Writer].
     */
    protected abstract fun newWriter(): Writer<V, D>

    /**
     *
     */
    fun getChannel(segment: SegmentId): FileChannel = this.channels.get(segment)

    /**
     * Closes this [FixedLengthFile] and the underlying channel.
     */
    override fun close() {
        this.channels.invalidateAll()
    }

    /**
     *
     */
    class WriterWeakReference(val writer: Writer<*,*>, queue: ReferenceQueue<Writer<*,*>>): WeakReference<Writer<*,*>>(writer, queue)
}