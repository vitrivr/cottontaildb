package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex64Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [XodusBinding] for [Complex64Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Complex64ValueXodusBinding: XodusBinding<Complex64Value> {
    override val type = Types.Complex64


    /**
     * [Complex64ValueXodusBinding] used for non-nullable values.
     */
    object NonNullable: Complex64ValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): Complex64Value {
            val stream = ByteArrayInputStream(entry.bytesUnsafe)
            return Complex64Value(DoubleBinding.BINDING.readObject(stream),DoubleBinding.BINDING.readObject(stream))
        }

        override fun valueToEntry(value: Complex64Value?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val stream = LightOutputStream(this.type.physicalSize)
            DoubleBinding.BINDING.writeObject(stream, value.real.value)
            DoubleBinding.BINDING.writeObject(stream, value.real.imaginary)
            return stream.asArrayByteIterable()
        }
    }

    /**
     * [Complex64ValueXodusBinding] used for nullable values.
     */
    object Nullable: Complex64ValueXodusBinding() {
        /** The special value that is being interpreted as NULL for this column. */
        private val NULL_VALUE = Complex64Value(Double.MIN_VALUE, Double.MIN_VALUE)

        /** The special value that is being interpreted as NULL for this column. */
        private val NULL_VALUE_RAW: ArrayByteIterable = NonNullable.valueToEntry(NULL_VALUE) as ArrayByteIterable

        override fun entryToValue(entry: ByteIterable): Complex64Value? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE_RAW.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                val stream = ByteArrayInputStream(bytesRead)
                Complex64Value(DoubleBinding.BINDING.readObject(stream),DoubleBinding.BINDING.readObject(stream))
            }
        }

        override fun valueToEntry(value: Complex64Value?): ByteIterable {
            if (value == null) return NULL_VALUE_RAW
            if (value == NULL_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            val stream = LightOutputStream(this.type.physicalSize)
            DoubleBinding.BINDING.writeObject(stream, value.real.value)
            DoubleBinding.BINDING.writeObject(stream, value.real.imaginary)
            return stream.asArrayByteIterable()
        }
    }
}