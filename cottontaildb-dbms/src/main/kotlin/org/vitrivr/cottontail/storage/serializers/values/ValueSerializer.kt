package org.vitrivr.cottontail.storage.serializers.values

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import java.io.ByteArrayInputStream
import org.vitrivr.cottontail.core.types.Value

/**
 * A serializer for Xodus based [Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
sealed interface ValueSerializer<T: Value> {

    /** The [Types] converted by this [ValueSerializer]. */
    val type: Types<T>

    /**
     * Converts a [ByteIterable] to a [Value].
     *
     * @param entry The [ByteIterable] to convert.
     * @return The resulting [Value].
     */
    fun fromEntry(entry: ByteIterable): T? = this.read(ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length))

    /**
     * Converts a [Value] to a [ByteIterable].
     *
     * @param value The [Value] to convert.
     * @return The resulting [ByteIterable].
     */
    fun toEntry(value: T): ByteIterable {
        val output = LightOutputStream()
        this.write(output, value)
        return output.asArrayByteIterable()
    }

    /**
     * Reads a [Value] from a [ByteArrayInputStream].
     *
     * @param input The [ByteArrayInputStream] to read.
     * @return The resulting [Value].
     */
    fun read(input: ByteArrayInputStream): T

    /**
     * Writes a [Value] to a [LightOutputStream].
     *
     * @param output The [LightOutputStream] to read.
     * @return The resulting [Value].
     */
    fun write(output: LightOutputStream, value: T)
}