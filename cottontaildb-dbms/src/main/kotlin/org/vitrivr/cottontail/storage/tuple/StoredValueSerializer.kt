package org.vitrivr.cottontail.storage.tuple

import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.OutOfLineValue
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.storage.ool.interfaces.AccessPattern
import org.vitrivr.cottontail.storage.ool.interfaces.OOLFile
import org.vitrivr.cottontail.storage.ool.interfaces.OOLReader
import org.vitrivr.cottontail.storage.ool.interfaces.OOLWriter
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import java.io.ByteArrayInputStream

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
     * Reads a [StoredValue] from the provided [ByteArrayInputStream].
     *
     * @param input The [ByteArrayInputStream] to read from.
     * @return The resulting [StoredValue].
     */
    fun read(input: ByteArrayInputStream): StoredValue<T>?

    /**
     * Writes a [StoredValue] to the provided [LightOutputStream].
     *
     * @param output The [LightOutputStream] to write from.
     * @param value The [StoredValue] to write.
     * @return The resulting [StoredValue].
     */
    fun write(output: LightOutputStream, value: T?)

    /**
     * A [StoredValueSerializer] that can handle [OutOfLineValue]s that are nullable.
     */
    @JvmInline
    value class Nullable<T: Value>(val wrapped: StoredValueSerializer<T>) : StoredValueSerializer<T> {
        override val type: Types<T>
            get() = this.wrapped.type

        override fun read(input: ByteArrayInputStream): StoredValue<T>? {
            input.mark(1)
            if (ByteBinding.BINDING.readObject(input) == NULL) {
                return null
            } else {
                input.reset()
                return this.wrapped.read(input)
            }
        }

        override fun write(output: LightOutputStream, value: T?) {
            if (value == null) {
                ByteBinding.BINDING.writeObject(output, NULL)
            } else {
                this.wrapped.write(output, value)
            }
        }
    }

    /**
     * A [StoredValueSerializer] that can handle [StoredValue.Inline]s.
     */
    class Inline<T: Value>(private val serializer: ValueSerializer<T>): StoredValueSerializer<T> {

        override val type: Types<T>
            get() = this.serializer.type

        override fun read(input: ByteArrayInputStream): StoredValue.Inline<T> {
            require(ByteBinding.BINDING.readObject(input) == INLINE) { "Wrong stored value flag: Expected INLINE flag."  }
            return StoredValue.Inline(this.serializer.read(input))
        }

        override fun write(output: LightOutputStream, value: T?) {
            ByteBinding.BINDING.writeObject(output, INLINE)
            this.serializer.write(output, value!!)
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

        override fun read(input: ByteArrayInputStream): StoredValue.OutOfLine.Fixed<T> {
            require(ByteBinding.BINDING.readObject(input) == VARIABLE) { "Wrong stored value flag: Expected VARIABLE flag." }
            return StoredValue.OutOfLine.Fixed(OutOfLineValue.Fixed(LongBinding.readCompressed(input)), this.reader)
        }

        override fun write(output: LightOutputStream, value: T?) {
            val ref = this.writer.append(value!!)
            ByteBinding.BINDING.writeObject(output, VARIABLE)
            LongBinding.writeCompressed(output, ref.rowId)
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

        override fun read(input: ByteArrayInputStream): StoredValue.OutOfLine.Variable<T> {
            require(ByteBinding.BINDING.readObject(input) == FIXED) { "Wrong stored value flag: Expected FIXED flag." }
            return StoredValue.OutOfLine.Variable(OutOfLineValue.Variable(LongBinding.readCompressed(input), IntegerBinding.readCompressed(input)), this.reader)
        }

        override fun write(output: LightOutputStream, value: T?) {
            val ref = this.writer.append(value!!)
            ByteBinding.BINDING.writeObject(output, FIXED)
            LongBinding.writeCompressed(output, ref.position)
            IntegerBinding.writeCompressed(output, ref.size)
        }

        fun flush() = this.writer.flush()
    }
}