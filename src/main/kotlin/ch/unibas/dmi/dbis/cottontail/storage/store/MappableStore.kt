package ch.unibas.dmi.dbis.cottontail.storage.store

import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

/**
 * An abstract representation over a facility that can hold data (a data [Store]) and allows for random access to that data.
 * Data may or may not be held persistently
 *
 * @version 1.0
 * @author Ralph Gasser
 */
interface MappableStore : Store {

    /**
     * Tries to map the given region of this [Store] and returns a [MappedByteBuffer] for that region. Can be used to
     * ,e.g., map a header of a file directly.
     *
     * @param start The start of the memory region map.
     * @param size The size of the memory region to map.
     * @param mode The [FileChannel.MapMode] to use.
     *
     * @return MappedByteBuffer
     */
    fun map(start: Long, size: Int, mode: FileChannel.MapMode = FileChannel.MapMode.READ_ONLY) : MappedByteBuffer
}