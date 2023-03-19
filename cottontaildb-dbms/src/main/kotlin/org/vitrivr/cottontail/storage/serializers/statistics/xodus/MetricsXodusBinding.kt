package org.vitrivr.cottontail.storage.serializers.statistics.xodus

import jetbrains.exodus.util.LightOutputStream
import org.mapdb.Serializer
import org.vitrivr.cottontail.dbms.statistics.statData.DataMetrics
import java.io.ByteArrayInputStream

/**
 * A [Serializer] for Xodus based [DataMetrics] serialization and deserialization.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.1
 */
interface MetricsXodusBinding<T: DataMetrics<*>> {

    /**
     *
     */
    fun read(stream: ByteArrayInputStream): T

    /**
     *
     */
    fun write(output: LightOutputStream, statistics: T)
}