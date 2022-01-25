package org.vitrivr.cottontail.functions.math.arithmetics.scalar

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.functions.*
import org.vitrivr.cottontail.core.functions.Function
import org.vitrivr.cottontail.core.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.functions.math.VectorDistance
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.NumericValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.functions.math.distance.binary.ChisquaredDistance
import kotlin.math.min

/**
 * A [VectorDistance] implementation to calculate the Chi^2 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Minimum<T : Value>(type: Types<T>): AbstractFunction<T>(Signature.Closed(FUNCTION_NAME, arrayOf(Argument.Typed(type), Argument.Typed(type)), type)) {

    companion object: AbstractFunctionGenerator<Value>() {
        private val FUNCTION_NAME = Name.FunctionName("min")

        override val signature: Signature.Open<out NumericValue<*>>
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun generateInternal(dst: Signature.Closed<*>): Function<NumericValue<*>> = when (dst.arguments[0].type) {
            is Types.Int -> Int()
            is Types.Long -> Long()
            is Types.Float -> Float()
            is Types.Double -> Double()
            else ->  throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $dst).")
        }
    }

    /**
     * [Minimum] for a [IntValue].
     */
    class Int: Minimum<IntValue>(Types.Int) {
        override val cost = 1.0f
        override fun invoke(): IntValue {
            val left = this.arguments[0] as IntValue
            val right = this.arguments[1] as IntValue
            return IntValue(min(left.value, right.value))
        }
    }

    /**
     * [Minimum] for a [LongValue].
     */
    class Long: Minimum<LongValue>(Types.Long) {
        override val cost = 1.0f
        override fun invoke(): LongValue {
            val left = this.arguments[0] as LongValue
            val right = this.arguments[1] as LongValue
            return LongValue(min(left.value, right.value))
        }
    }

    /**
     * [Minimum] for a [FloatValue].
     */
    class Float: Minimum<FloatValue>(Types.Float) {
        override val cost = 1.0f
        override fun invoke(): FloatValue {
            val left = this.arguments[0] as FloatValue
            val right = this.arguments[1] as FloatValue
            return FloatValue(min(left.value, right.value))
        }
    }

    /**
     * (Element-wise) [Minimum] for a [IntValue].
     */
    class Double: Minimum<DoubleValue>(Types.Double) {
        override val cost = 1.0f
        override fun invoke(): DoubleValue {
            val left = this.arguments[0] as DoubleValue
            val right = this.arguments[1] as DoubleValue
            return DoubleValue(min(left.value, right.value))
        }
    }
}