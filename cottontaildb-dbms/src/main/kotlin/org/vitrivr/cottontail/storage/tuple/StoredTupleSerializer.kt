package org.vitrivr.cottontail.storage.tuple

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.OutOfLineValue
import org.vitrivr.cottontail.dbms.entity.values.StoredTuple
import org.vitrivr.cottontail.storage.ool.interfaces.AccessPattern
import org.vitrivr.cottontail.storage.ool.interfaces.OOLFile
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import org.xerial.snappy.Snappy
import java.io.ByteArrayInputStream

/**
 * A serializer for [StoredTuple]s that uses a [StoredValueSerializer] to serialize individual [Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Suppress("UNCHECKED_CAST")
class StoredTupleSerializer(val columns: Array<ColumnDef<*>>, private val files: Map<Name.ColumnName, OOLFile<*, *>>, private val pattern: AccessPattern = AccessPattern.RANDOM) {

    /** The [StoredValueSerializer]s used by this [StoredTupleSerializer]. */
    private val serializers = this.columns.map {
        val core = if (it.type.inline) {
            StoredValueSerializer.Inline(SerializerFactory.value(it.type) as ValueSerializer<*>)
        } else if (it.type.fixedLength) {
            StoredValueSerializer.Fixed(this.files[it.name] as OOLFile<Value, OutOfLineValue.Fixed>, this.pattern)
        } else {
            StoredValueSerializer.Variable(this.files[it.name] as OOLFile<Value, OutOfLineValue.Variable>, this.pattern)
        }
        if (it.nullable) {
            StoredValueSerializer.Nullable(core)
        } else {
            core
        }
    }

    /**
     * Reads a [StoredTuple] from a [ByteIterable].
     *
     *  @param tupleId [TupleId] of the [StoredTuple]
     * @param entry [ByteIterable] to read.
     */
    fun fromEntry(tupleId: TupleId, entry: ByteIterable): StoredTuple {
        val input = ByteArrayInputStream(Snappy.uncompress(entry.bytesUnsafe))
        return StoredTuple(tupleId, this.columns, Array(this.serializers.size) {  this.serializers[it].read(input) })
    }

    /**
     * Writes a [StoredTuple] to a [ByteIterable].
     *
     * @param tuple [StoredTuple] to write
     * @return [ByteIterable]
     */
    fun toEntry(tuple: Tuple): ByteIterable {
        val output = LightOutputStream()
        for ((c, s) in this.columns.zip(this.serializers)) {
            val value = tuple[c]
            (s as StoredValueSerializer<Value>).write(output, value)
        }
        return ArrayByteIterable(Snappy.compress(output.bufferBytes))
    }

    /**
     * Flushes the content of this [StoredTupleSerializer] to disk.
     */
    fun flush() = this.serializers.forEach {
        val check = if (it is StoredValueSerializer.Nullable<*>) it.wrapped else it
        when (check) {
            is StoredValueSerializer.Fixed<*> -> check.flush()
            is StoredValueSerializer.Variable<*> -> check.flush()
            else -> { /* No-op */ }
        }
    }
}