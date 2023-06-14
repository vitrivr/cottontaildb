package org.vitrivr.cottontail.storage.serializers.statistics.xodus

import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import java.io.ByteArrayInputStream

/**
 * A serializer for Xodus based [ValueStatistics] serialization and deserialization.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.0.1
 */
interface MetricsXodusBinding<T: ValueStatistics<*>> {

    /**
     * Reads a [ValueStatistics] from the given [ByteArrayInputStream].
     *
     * @param stream [ByteArrayInputStream] to read from
     * @return [ValueStatistics]
     */
    fun read(stream: ByteArrayInputStream): T

    /**
     * Writes a [ValueStatistics] to the given [LightOutputStream].
     *
     * @param output The [LightOutputStream] to write to.
     * @param statistics The [ValueStatistics] to write.
     */
    fun write(output: LightOutputStream, statistics: T)
}