package org.vitrivr.cottontail.core.queries.functions.math.arithmetics.scalar

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.LongValue
import kotlin.math.max

/**
 * A [Function] to calculate the maximum of two scalar values.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Maximum<T : Value>(val type: Types<T>): Function<T> {

    companion object: FunctionGenerator<Value> {
        private val FUNCTION_NAME = Name.FunctionName("max")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Numeric, Argument.Numeric))

        override fun obtain(signature: Signature.SemiClosed): Maximum<*> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            return when(signature.arguments[0].type) {
                is Types.Int  -> Int()
                is Types.Long -> Long()
                is Types.Float -> Float()
                is Types.Double -> Double()
                else -> throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            }
        }

        override fun resolve(signature: Signature.Open): List<Signature.Closed<*>> {
            if (Companion.signature != signature) throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            return listOf(
                Int().signature,
                Long().signature,
                Float().signature,
                Double().signature,
            )
        }
    }

    /** The [Cost] of executing this [Maximum]. */
    override val cost
        get() = (Cost.MEMORY_ACCESS * 2)

    /** The [Signature.Closed] of this [Maximum]. */
    override val signature: Signature.Closed<T> = Signature.Closed(FUNCTION_NAME, arrayOf(this.type, this.type), this.type)

    /**
     * [Maximum] for a [IntValue].
     */
    class Int: Maximum<IntValue>(Types.Int) {
        override fun invoke(vararg arguments: Value?): IntValue {
            val left = arguments[0] as IntValue
            val right = arguments[1] as IntValue
            return IntValue(max(left.value, right.value))
        }
    }

    /**
     * [Maximum] for a [LongValue].
     */
    class Long: Maximum<LongValue>(Types.Long)  {
        override fun invoke(vararg arguments: Value?): LongValue {
            val left = arguments[0] as LongValue
            val right = arguments[1] as LongValue
            return LongValue(max(left.value, right.value))
        }
    }

    /**
     * [Maximum] for a [FloatValue].
     */
    class Float: Maximum<FloatValue>(Types.Float) {
        override fun invoke(vararg arguments: Value?): FloatValue {
            val left = arguments[0] as FloatValue
            val right = arguments[1] as FloatValue
            return FloatValue(max(left.value, right.value))
        }
    }

    /**
     * (Element-wise) [Maximum] for a [IntValue].
     */
    class Double: Maximum<DoubleValue>(Types.Double) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val left = arguments[0] as DoubleValue
            val right = arguments[1] as DoubleValue
            return DoubleValue(max(left.value, right.value))
        }
    }
}