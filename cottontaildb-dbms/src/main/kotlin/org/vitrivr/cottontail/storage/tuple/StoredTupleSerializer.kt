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
import java.lang.Math.floorDiv
import java.nio.ByteBuffer

/**
 * A serializer for [StoredTuple]s that uses a [StoredValueSerializer] to serialize individual [Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Suppress("UNCHECKED_CAST")
class StoredTupleSerializer(val columns: Array<ColumnDef<*>>, private val files: Map<Name.ColumnName, OOLFile<*, *>>, private val pattern: AccessPattern = AccessPattern.RANDOM) {

    companion object {

        const val NULL: Int = 0 // 00
        const val INLINE: Int = 1 // 01
        const val FIXED: Int = 2 // 10
        const val VARIABLE: Int = 3 // 11

        /**
         * Encodes the states from a [ByteArray].
         *
         * @param states Array of states.
         * @return [ByteArray] of encoded states.
         */
        fun encode(states: IntArray, into: ByteArray) {
            require(into.size >= -floorDiv(-states.size, 2)) { "Invalid number of states." }
            for (i in states.indices) {
                into[i / 4] = (into[i / 4].toInt() or (states[i] shl ((i % 4) * 2))).toByte()
            }
        }

        /**
         * Decodes the states from a [ByteArray].
         *
         * @param encoded Encoded [ByteArray]
         * @param into [IntArray] of states.
         */
        fun decode(encoded: ByteArray, into: IntArray) {
            require(encoded.size >= -floorDiv(-into.size, 2)) { "Invalid number of states." }
            for (i in into.indices) {
                into[i] = (encoded[i / 4].toInt() ushr ((i % 4) * 2)) and 3
            }
        }
    }

    /** The [StoredValueSerializer]s used by this [StoredTupleSerializer]. */
    private val serializers = this.columns.map {
        if (it.type.inline) {
            StoredValueSerializer.Inline(ValueBinding(ValueSerializer.serializer(it.type)) as ValueBinding<*>)
        } else if (it.type.fixedLength) {
            StoredValueSerializer.Fixed(this.files[it.name] as OOLFile<Value, OutOfLineValue.Fixed>, this.pattern)
        } else {
            StoredValueSerializer.Variable(this.files[it.name] as OOLFile<Value, OutOfLineValue.Variable>, this.pattern)
        }
    }

    /** The estimated size of the data produced by this [StoredValueSerializer]. */
    private val tupleSize by lazy {
        -floorDiv(-this.columns.size, 2) + this.columns.sumOf {
            if (it.type.inline) {
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

    /** A [ByteArray] to hold the encoded header of every entry. */
    private val headerByteBuffer = ByteArray(-floorDiv(-this.columns.size, 2))

    /** A [ByteArray] to hold the decoded header of every entry. */
    private val headerBuffer = IntArray(this.columns.size)

    /**
     * Reads a [StoredTuple] from a [ByteIterable].
     *
     *  @param tupleId [TupleId] of the [StoredTuple]
     * @param entry [ByteIterable] to read.
     */
    fun fromEntry(tupleId: TupleId, entry: ByteIterable): StoredTuple {
        val input = ByteBuffer.wrap(Snappy.uncompress(entry.bytesUnsafe))

        /* Read header. */
        input.get(this.headerByteBuffer)
        decode(this.headerByteBuffer, this.headerBuffer)

        /* Read stored tuple */
        return StoredTuple(tupleId, this.columns, Array(this.headerBuffer.size) {
            when (this.headerBuffer[it]) {
                NULL -> null
                INLINE -> this.serializers[it].read(input)
                VARIABLE -> this.serializers[it].read(input)
                FIXED -> this.serializers[it].read(input)
                else -> throw IllegalArgumentException("Invalid header in StoredTuple.")
            }
        })
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

        /* Prepare & write header. */
        for ((i, c) in this.columns.withIndex()) {
            val value = tuple[c]
            if (value == null) {
               this.headerBuffer[i] = NULL
            } else if (c.type.inline) {
               this.headerBuffer[i] = INLINE
            } else if (c.type.fixedLength) {
               this.headerBuffer[i] = FIXED
            } else {
               this.headerBuffer[i] = VARIABLE
            }
        }
        encode(this.headerBuffer, this.headerByteBuffer)
        output.write(this.headerByteBuffer)

        /* Encode values. */
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
        when (it) {
            is StoredValueSerializer.Fixed<*> -> it.flush()
            is StoredValueSerializer.Variable<*> -> it.flush()
            else -> { /* No-op */ }
        }
    }
}