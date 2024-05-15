package org.vitrivr.cottontail.storage.tuple

import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.dbms.entity.values.StoredValueRef
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import java.io.ByteArrayInputStream

/**
 * A serializer for [StoredValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface StoredValueRefSerializer<T: StoredValueRef> {
    companion object {
        private const val NULL = 0.toByte()
        private const val INLINE = 1.toByte()
        private const val FIXED = 2.toByte()
        private const val VARIABLE = 3.toByte()
    }

    /**
     * Reads a [StoredValue] from the provided [ByteArrayInputStream].
     *
     * @param input The [ByteArrayInputStream] to read from.
     * @return The resulting [StoredValue].
     */
    fun read(input: ByteArrayInputStream): T

    /**
     * Writes a [StoredValue] to the provided [LightOutputStream].
     *
     * @param output The [LightOutputStream] to write from.
     * @param value The [StoredValue] to write.
     * @return The resulting [StoredValue].
     */
    fun write(output: LightOutputStream, value: T)

    /**
     *
     */
    @JvmInline
    value class Nullable(private val wrapped: StoredValueRefSerializer<StoredValueRef>) : StoredValueRefSerializer<StoredValueRef> {
        override fun read(input: ByteArrayInputStream): StoredValueRef = if (ByteBinding.BINDING.readObject(input) == NULL) {
            StoredValueRef.Null
        } else {
            this.wrapped.read(input)
        }

        override fun write(output: LightOutputStream, value: StoredValueRef) {
            if (value == StoredValueRef.Null) {
                ByteBinding.BINDING.writeObject(output, NULL)
            } else {
                this.wrapped.write(output, value)
            }
        }
    }

    /**
     *
     */
    class Inline<T: org.vitrivr.cottontail.core.types.Value>(private val serializer: ValueSerializer<T>): StoredValueRefSerializer<StoredValueRef.Inline<T>> {
        override fun read(input: ByteArrayInputStream): StoredValueRef.Inline<T> {
            require(ByteBinding.BINDING.readObject(input) == INLINE) { }
            return StoredValueRef.Inline(this.serializer.read(input))
        }

        override fun write(output: LightOutputStream, value: StoredValueRef.Inline<T>) {
            ByteBinding.BINDING.writeObject(output, INLINE)
            this.serializer.write(output, value.value)
        }
    }


    /**
     *
     */
    data object Fixed: StoredValueRefSerializer<StoredValueRef.OutOfLine.Fixed> {
        override fun read(input: ByteArrayInputStream): StoredValueRef.OutOfLine.Fixed {
            require(ByteBinding.BINDING.readObject(input) == VARIABLE) { }
            return StoredValueRef.OutOfLine.Fixed(LongBinding.readCompressed(input))
        }

        override fun write(output: LightOutputStream, value: StoredValueRef.OutOfLine.Fixed) {
            ByteBinding.BINDING.writeObject(output, VARIABLE)
            LongBinding.writeCompressed(output, value.rowId)
        }
    }


    /**
     *
     */
    data object Variable: StoredValueRefSerializer<StoredValueRef.OutOfLine.Variable> {
        override fun read(input: ByteArrayInputStream): StoredValueRef.OutOfLine.Variable {
            require(ByteBinding.BINDING.readObject(input) == FIXED) { }
            return StoredValueRef.OutOfLine.Variable(LongBinding.readCompressed(input), IntegerBinding.readCompressed(input))
        }

        override fun write(output: LightOutputStream, value: StoredValueRef.OutOfLine.Variable) {
            ByteBinding.BINDING.writeObject(output, FIXED)
            LongBinding.writeCompressed(output, value.position)
            IntegerBinding.writeCompressed(output, value.size)
        }
    }
}