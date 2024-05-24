package org.vitrivr.cottontail.storage.tuple

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.OutOfLineValue
import org.vitrivr.cottontail.dbms.entity.values.StoredTuple
import org.vitrivr.cottontail.serialization.buffer.ValueSerializer
import org.vitrivr.cottontail.storage.ool.interfaces.AccessPattern
import org.vitrivr.cottontail.storage.ool.interfaces.OOLFile
import org.vitrivr.cottontail.storage.serializers.ValueBinding
import org.xerial.snappy.Snappy
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer

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
            StoredValueSerializer.Inline(ValueBinding(ValueSerializer.serializer(it.type)) as ValueBinding<*>)
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

    /** The estimated size of the data produced by this [StoredValueSerializer]. */
    private val tupleSize by lazy {
        this.columns.sumOf {
            1 + if (it.type.inline) {
                it.type.physicalSize
            } else if (it.type.fixedLength) {
                Long.SIZE_BYTES                  /* Row ID. */
            } else {
                Long.SIZE_BYTES + Int.SIZE_BYTES /* Position and length. */
            }
        }
    }

    /** A [ByteArrayOutputStream] used to serialize output. */
    private val outputBuffer by lazy { ByteArrayOutputStream(this.tupleSize) }

    /**
     * Reads a [StoredTuple] from a [ByteIterable].
     *
     *  @param tupleId [TupleId] of the [StoredTuple]
     * @param entry [ByteIterable] to read.
     */
    fun fromEntry(tupleId: TupleId, entry: ByteIterable): StoredTuple {
        val input = ByteBuffer.wrap(Snappy.uncompress(entry.bytesUnsafe))
        return StoredTuple(tupleId, this.columns, Array(this.serializers.size) {  this.serializers[it].read(input) })
    }

    /**
     * Writes a [StoredTuple] to a [ByteIterable].
     *
     * @param tuple [StoredTuple] to write
     * @return [ByteIterable]
     */
    fun toEntry(tuple: Tuple): ByteIterable {
        this.outputBuffer.reset()
        val output = DataOutputStream(this.outputBuffer)
        for ((c, s) in this.columns.zip(this.serializers)) {
            val value = tuple[c]
            (s as StoredValueSerializer<Value>).write(output, value)
        }
        return ArrayByteIterable(Snappy.compress(this.outputBuffer.toByteArray()))
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