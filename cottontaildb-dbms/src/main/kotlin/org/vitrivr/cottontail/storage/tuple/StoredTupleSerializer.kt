package org.vitrivr.cottontail.storage.tuple

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.StoredTuple
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.dbms.entity.values.StoredValueRef
import org.vitrivr.cottontail.storage.entries.interfaces.Reader
import org.vitrivr.cottontail.storage.entries.interfaces.Writer
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import org.xerial.snappy.Snappy
import java.io.ByteArrayInputStream

/**
 * A serializer for [StoredTuple]s that uses a [StoredValueRefSerializer] to serialize individual [Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Suppress("UNCHECKED_CAST")
class StoredTupleSerializer(val columns: Array<ColumnDef<*>>, private val writers: Map<Name.ColumnName, Writer<*, *>>, private val readers: Map<Name.ColumnName, Reader<*, *>>) {

    /** The [StoredValueRefSerializer]s used by this [StoredTupleSerializer]. */
    private val serializers = this.columns.map {
        val core = when (it.type) {
            is Types.Vector<*,*> -> StoredValueRefSerializer.Fixed
            is Types.String,
            is Types.ByteString -> StoredValueRefSerializer.Variable
            else -> StoredValueRefSerializer.Inline(SerializerFactory.value(it.type) as ValueSerializer<*>)
        }
        if (it.nullable) {
            StoredValueRefSerializer.Nullable(core as StoredValueRefSerializer<StoredValueRef>)
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
        return StoredTuple(tupleId, this.columns, Array(this.serializers.size) {  toValue(this.columns[it], this.serializers[it].read(input)) })
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
            when (s) {
                is StoredValueRefSerializer.Fixed -> {
                    val writer = this.writers[c.name] as Writer<Value, StoredValueRef.OutOfLine.Fixed>
                    val ref = writer.append(value as Value)
                    s.write(output, ref)
                }
                is StoredValueRefSerializer.Variable -> {
                    val writer = this.writers[c.name] as Writer<Value, StoredValueRef.OutOfLine.Variable>
                    val ref = writer.append(value as Value)
                    s.write(output, ref)
                }
                is StoredValueRefSerializer.Inline<*> -> (s as StoredValueRefSerializer.Inline<Value>).write(output,  StoredValueRef.Inline(value!!))
                is StoredValueRefSerializer.Nullable -> s.write(output, value?.let { StoredValueRef.Inline(it) } ?: StoredValueRef.Null)
            }
        }
        return ArrayByteIterable(Snappy.compress(output.bufferBytes))
    }

    /**
     * Flushes the content of this [StoredTupleSerializer] to disk.
     */
    fun flush() = this.writers.values.forEach { it.flush() }

    /**
     *
     */
    private fun toValue(column: ColumnDef<*>, ref: StoredValueRef) = when(ref) {
        StoredValueRef.Null -> null
        is StoredValueRef.Inline<*> -> StoredValue.Inline(ref.value)
        is StoredValueRef.OutOfLine.Fixed -> StoredValue.OutOfLine.Fixed(ref, this.readers[column.name] as? Reader<Value, StoredValueRef.OutOfLine.Fixed> ?: throw IllegalStateException("No reader found for column ${column.name}!"))
        is StoredValueRef.OutOfLine.Variable -> StoredValue.OutOfLine.Variable(ref, this.readers[column.name] as? Reader<Value, StoredValueRef.OutOfLine.Variable> ?: throw IllegalStateException("No reader found for column ${column.name}!"))
    }
}