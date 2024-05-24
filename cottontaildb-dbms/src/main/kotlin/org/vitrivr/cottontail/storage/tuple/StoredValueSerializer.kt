package org.vitrivr.cottontail.storage.tuple

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.OutOfLineValue
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.storage.ool.interfaces.AccessPattern
import org.vitrivr.cottontail.storage.ool.interfaces.OOLFile
import org.vitrivr.cottontail.storage.ool.interfaces.OOLReader
import org.vitrivr.cottontail.storage.ool.interfaces.OOLWriter
import org.vitrivr.cottontail.storage.serializers.ValueBinding
import java.io.DataOutputStream
import java.nio.ByteBuffer

/**
 * A serializer for [Value]s and deserializer for [StoredValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface StoredValueSerializer<T: Value> {
    companion object {
        private const val NULL = 0.toByte()
        private const val INLINE = 1.toByte()
        private const val FIXED = 2.toByte()
        private const val VARIABLE = 3.toByte()
    }

    /** The [Types] of [OutOfLineValue]. */
    val type: Types<T>

    /**
     * Reads a [StoredValue] from the provided [ByteBuffer].
     *
     * @param input The [ByteBuffer] to read from.
     * @return The resulting [StoredValue].
     */
    fun read(input: ByteBuffer): StoredValue<T>?

    /**
     * Writes a [StoredValue] to the provided [DataOutputStream].
     *
     * @param output The [DataOutputStream] to write to.
     * @param value The [StoredValue] to write.
     */
    fun write(output: DataOutputStream, value: T?)

    /**
     * A [StoredValueSerializer] that can handle [OutOfLineValue]s that are nullable.
     */
    @JvmInline
    value class Nullable<T: Value>(val wrapped: StoredValueSerializer<T>) : StoredValueSerializer<T> {
        override val type: Types<T>
            get() = this.wrapped.type

        override fun read(input: ByteBuffer): StoredValue<T>? {
            if (input.get() == NULL) {
                return null
            } else {
                input.reset()
                return this.wrapped.read(input)
            }
        }

        override fun write(output: DataOutputStream, value: T?) {
            if (value == null) {
                output.writeByte(NULL.toInt())
            } else {
                this.wrapped.write(output, value)
            }
        }
    }

    /**
     * A [StoredValueSerializer] that can handle [StoredValue.Inline]s.
     */
    class Inline<T: Value>(private val binding: ValueBinding<T>): StoredValueSerializer<T> {
        override val type: Types<T>
            get() = this.binding.serializer.type

        override fun read(input: ByteBuffer): StoredValue.Inline<T> {
            require(input.get() == INLINE) { "Wrong stored value flag: Expected INLINE flag."  }
            return StoredValue.Inline(this.binding.read(input))
        }

        override fun write(output: DataOutputStream, value: T?) {
            output.writeByte(INLINE.toInt())
            this.binding.write(output, value!!)
        }
    }


    /**
     * A [StoredValueSerializer] that can handle [StoredValue.OutOfLine.Fixed]s.
     */
    class Fixed<T: Value>(private val file: OOLFile<T, OutOfLineValue.Fixed>, val pattern: AccessPattern = AccessPattern.RANDOM): StoredValueSerializer<T> {
        /** The [OOLReader] used by this [Fixed] [StoredValueSerializer]. */
        private val reader by lazy { this.file.reader(this.pattern) }

        /** The [OOLWriter] used by this [Fixed] [StoredValueSerializer]. */
        private val writer by lazy { this.file.writer() }

        /** The [Types] of this [Fixed] [StoredValueSerializer]. */
        override val type: Types<T>
            get() = this.file.type

        override fun read(input: ByteBuffer): StoredValue.OutOfLine.Fixed<T> {
            require(input.get() == VARIABLE) { "Wrong stored value flag: Expected VARIABLE flag." }
            return StoredValue.OutOfLine.Fixed(OutOfLineValue.Fixed(input.getLong()), this.reader)
        }

        override fun write(output: DataOutputStream, value: T?) {
            val ref = this.writer.append(value!!)
            output.writeByte(VARIABLE.toInt())
            output.writeLong(ref.rowId)
        }

        fun flush() = this.writer.flush()
    }


    /**
     * A [StoredValueSerializer] that can handle [StoredValue.OutOfLine.Variable]s.
     */
    class Variable<T: Value>(private val file: OOLFile<T, OutOfLineValue.Variable>, val pattern: AccessPattern = AccessPattern.RANDOM): StoredValueSerializer<T> {
        /** The [OOLReader] used by this [Fixed] [StoredValueSerializer]. */
        private val reader by lazy { this.file.reader(this.pattern) }

        /** The [OOLWriter] used by this [Fixed] [StoredValueSerializer]. */
        private val writer by lazy { this.file.writer() }

        /** The [Types] of this [Fixed] [StoredValueSerializer]. */
        override val type: Types<T>
            get() = this.file.type

        override fun read(input: ByteBuffer): StoredValue.OutOfLine.Variable<T> {
            require(input.get() == FIXED) { "Wrong stored value flag: Expected FIXED flag." }
            return StoredValue.OutOfLine.Variable(OutOfLineValue.Variable(input.getLong(), input.getInt()), this.reader)
        }

        override fun write(output: DataOutputStream, value: T?) {
            val ref = this.writer.append(value!!)
            output.writeByte(FIXED.toInt())
            output.writeLong(ref.position)
            output.writeInt(ref.size)
        }

        fun flush() = this.writer.flush()
    }
}