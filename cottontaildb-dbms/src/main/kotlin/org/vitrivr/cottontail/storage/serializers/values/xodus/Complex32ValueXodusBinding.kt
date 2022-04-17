package org.vitrivr.cottontail.storage.serializers.values.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex32Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream

/**
 * A [XodusBinding] for [Complex32Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Complex32ValueXodusBinding: XodusBinding<Complex32Value> {
    override val type = Types.Complex32

    object NonNullable: Complex32ValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): Complex32Value {
            val stream = ByteArrayInputStream(entry.bytesUnsafe)
            return Complex32Value(SignedFloatBinding.BINDING.readObject(stream), SignedFloatBinding.BINDING.readObject(stream))
        }

        override fun valueToEntry(value: Complex32Value?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val stream = LightOutputStream(this.type.physicalSize)
            SignedFloatBinding.BINDING.writeObject(stream, value.real.value)
            SignedFloatBinding.BINDING.writeObject(stream, value.real.imaginary)
            return stream.asArrayByteIterable()
        }
    }

    object Nullable: Complex32ValueXodusBinding() {
        /** The special value that is being interpreted as NULL for this column. */
        private val NULL_VALUE = Complex32Value(Float.MIN_VALUE, Float.MIN_VALUE)

        /** The special value that is being interpreted as NULL for this column. */
        private val NULL_VALUE_RAW = NonNullable.valueToEntry(NULL_VALUE)

        override fun entryToValue(entry: ByteIterable): Complex32Value? {
            if (entry == NULL_VALUE_RAW) return null
            val stream = ByteArrayInputStream(entry.bytesUnsafe)
            return Complex32Value(SignedFloatBinding.BINDING.readObject(stream), SignedFloatBinding.BINDING.readObject(stream))
        }

        override fun valueToEntry(value: Complex32Value?): ByteIterable {
            if (value == null) return NULL_VALUE_RAW
            if (value == NULL_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            val stream = LightOutputStream(this.type.physicalSize)
            SignedFloatBinding.BINDING.writeObject(stream, value.real.value)
            SignedFloatBinding.BINDING.writeObject(stream, value.real.imaginary)
            return stream.asArrayByteIterable()
        }
    }

}