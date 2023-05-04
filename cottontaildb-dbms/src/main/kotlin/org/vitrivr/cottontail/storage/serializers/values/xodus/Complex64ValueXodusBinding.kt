package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64Value
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream

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
            return Complex64Value(SignedDoubleBinding.BINDING.readObject(stream),SignedDoubleBinding.BINDING.readObject(stream))
        }

        override fun valueToEntry(value: Complex64Value?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val stream = LightOutputStream(this.type.physicalSize)
            SignedDoubleBinding.BINDING.writeObject(stream, value.real.value)
            SignedDoubleBinding.BINDING.writeObject(stream, value.real.imaginary)
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
        private val NULL_VALUE_RAW = NonNullable.valueToEntry(NULL_VALUE)

        override fun entryToValue(entry: ByteIterable): Complex64Value? {
            if (entry == NULL_VALUE_RAW) return null
            val stream = ByteArrayInputStream(entry.bytesUnsafe)
            return Complex64Value(SignedDoubleBinding.BINDING.readObject(stream),SignedDoubleBinding.BINDING.readObject(stream))
        }

        override fun valueToEntry(value: Complex64Value?): ByteIterable {
            if (value == null) return NULL_VALUE_RAW
            if (value == NULL_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            val stream = LightOutputStream(this.type.physicalSize)
            SignedDoubleBinding.BINDING.writeObject(stream, value.real.value)
            SignedDoubleBinding.BINDING.writeObject(stream, value.real.imaginary)
            return stream.asArrayByteIterable()
        }
    }
}