package org.vitrivr.cottontail.dbms.functions.math.arithmetics.vector

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.NumericValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * Generates the vector sum over the given [VectorValue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Sum<T : NumericValue<*>>(val type: Types.Vector<*,T>): Function<T> {

    companion object : FunctionGenerator<NumericValue<*>> {
        private val FUNCTION_NAME = Name.FunctionName("vsum")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Sum<*> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            return when(val type = signature.arguments[0].type) {
                is Types.DoubleVector -> DoubleVector(type)
                is Types.FloatVector -> FloatVector(type)
                is Types.LongVector -> LongVector(type)
                is Types.IntVector -> IntVector(type)
                else -> throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            }
        }

        override fun resolve(signature: Signature.Open): List<Signature.Closed<*>> {
            if (Companion.signature != signature) throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            return listOf(
                DoubleVector(Types.DoubleVector(1)).signature,
                FloatVector(Types.FloatVector(1)).signature,
                LongVector(Types.LongVector(1)).signature,
                IntVector(Types.IntVector(1)).signature
            )
        }
    }

    /** The [Cost] of executing this element-wise vector [Sum]. */
    override val cost: Cost
        get() = (Cost.FLOP + Cost.MEMORY_ACCESS) * this.d

    /** The [Signature.Closed] of this vector [Sum]. */
    override val signature = Signature.Closed(FUNCTION_NAME, arrayOf(this.type), this.type.elementType)

    /** The dimensionality of this vector [Sum]. */
    val d: Int
        get() = this.type.logicalSize

    /**
     * (Element-wise) [Maximum] for a [IntVectorValue].
     */
    class IntVector(type: Types.IntVector): Sum<IntValue>(type) {
        override fun invoke(vararg arguments: Value?): IntValue {
            val argument = arguments[0] as IntVectorValue
            return IntValue(argument.data.sum())
        }
    }

    /**
     * (Element-wise) [Maximum] for a [LongVectorValue].
     */
    class LongVector(type: Types.LongVector): Sum<LongValue>(type) {
        override fun invoke(vararg arguments: Value?): LongValue {
            val argument = arguments[0] as LongVectorValue
            return LongValue(argument.data.sum())
        }
    }

    /**
     * (Element-wise) [Maximum] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.FloatVector): Sum<FloatValue>(type) {
        override fun invoke(vararg arguments: Value?): FloatValue {
            val argument = arguments[0] as FloatVectorValue
            return FloatValue(argument.data.sum())
        }
    }

    /**
     * (Element-wise) [Maximum] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.DoubleVector): Sum<DoubleValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val argument = arguments[0] as DoubleVectorValue
            return DoubleValue(argument.data.sum())
        }
    }
}