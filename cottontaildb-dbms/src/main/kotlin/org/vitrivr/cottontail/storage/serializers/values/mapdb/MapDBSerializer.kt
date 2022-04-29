package org.vitrivr.cottontail.storage.serializers.values.mapdb

import org.mapdb.Serializer
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A [Serializer] for MapDB based [Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface MapDBSerializer<T : Value> : Serializer<T> {
    override fun isTrusted(): Boolean = true
}