package org.vitrivr.cottontail.functions.math.arithmetics.scalar

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.functions.math.VectorDistance
import org.vitrivr.cottontail.core.queries.functions.*
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue
import kotlin.math.min

/**
 * A [VectorDistance] implementation to calculate the Chi^2 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Minimum<T : Value>(val type: Types<T>): Function<T> {

    companion object: FunctionGenerator<Value> {
        private val FUNCTION_NAME = Name.FunctionName("min")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Numeric, Argument.Numeric))

        override fun obtain(signature: Signature.SemiClosed): Minimum<*> {
            check(this.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${this.signature}. This is a programmer's error!"  }
            return when(signature.arguments[0].type) {
                is Types.Int  -> Int()
                is Types.Long -> Long()
                is Types.Float -> Float()
                is Types.Double -> Double()
                else -> throw FunctionNotSupportedException("Function generator ${this.signature} cannot generate function with signature $signature.")
            }
        }

        override fun resolve(signature: Signature.Open): List<Signature.Closed<*>> {
            if (this.signature != signature) throw FunctionNotSupportedException("Function generator ${this.signature} cannot generate function with signature $signature.")
            return listOf(
                Int().signature,
                Long().signature,
                Float().signature,
                Double().signature,
            )
        }
    }

    /** The CPU cost of executing this [Minimum] is 1.0. */
    override val cost = 1.0f

    /** The [Signature.Closed] of this [Minimum]. */
    override val signature: Signature.Closed<T> = Signature.Closed(FUNCTION_NAME, arrayOf(this.type, this.type), this.type)

    /**
     * [Minimum] for a [IntValue].
     */
    class Int: Minimum<IntValue>(Types.Int) {
        override fun invoke(vararg arguments: Value?): IntValue {
            val left = arguments[0] as IntValue
            val right = arguments[1] as IntValue
            return IntValue(min(left.value, right.value))
        }
    }

    /**
     * [Minimum] for a [LongValue].
     */
    class Long: Minimum<LongValue>(Types.Long) {
        override fun invoke(vararg arguments: Value?): LongValue {
            val left = arguments[0] as LongValue
            val right = arguments[1] as LongValue
            return LongValue(min(left.value, right.value))
        }
    }

    /**
     * [Minimum] for a [FloatValue].
     */
    class Float: Minimum<FloatValue>(Types.Float) {
        override fun invoke(vararg arguments: Value?): FloatValue {
            val left = arguments[0] as FloatValue
            val right = arguments[1] as FloatValue
            return FloatValue(min(left.value, right.value))
        }
    }

    /**
     * (Element-wise) [Minimum] for a [IntValue].
     */
    class Double: Minimum<DoubleValue>(Types.Double) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val left = arguments[0] as DoubleValue
            val right = arguments[1] as DoubleValue
            return DoubleValue(min(left.value, right.value))
        }
    }
}