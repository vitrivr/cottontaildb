package org.vitrivr.cottontail.dbms.functions.math.arithmetics.vector

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A [Function] to multiply a vector with a scalar.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Multiplication<T : VectorValue<*>>(val type: Types.Vector<T,*>): Function<T> {

    companion object : FunctionGenerator<VectorValue<*>> {
        private val FUNCTION_NAME = Name.FunctionName("mul")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Scalar))

        override fun obtain(signature: Signature.SemiClosed): Multiplication<*> {
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

    /** The CPU cost of executing this [Minimum] is 1.0. */
    override val cost = 1.0f * this.d

    /** The [Signature.Closed] of this [Minimum]. */
    override val signature = Signature.Closed(FUNCTION_NAME, arrayOf(this.type, this.type.elementType), this.type)

    /** The dimensionality of this [Minimum]. */
    val d: Int
        get() = this.type.logicalSize

    /**
     * Multiplication of a [IntVectorValue] with a [IntValue].
     */
    class IntVector(type: Types.IntVector): Multiplication<IntVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): IntVectorValue {
            val left = arguments[0] as IntVectorValue
            val right = arguments[1] as IntValue
            return IntVectorValue(IntArray(this.d) { left[it].value * right.value })
        }
    }

    /**
     * Multiplication of a [LongVectorValue] with a [LongValue].
     */
    class LongVector(type: Types.LongVector): Multiplication<LongVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): LongVectorValue {
            val left = arguments[0] as LongVectorValue
            val right = arguments[1] as LongValue
            return LongVectorValue(LongArray(this.d) { left[it].value * right.value })
        }
    }

    /**
     * Multiplication of a [FloatVectorValue] with a [FloatValue].
     */
    class FloatVector(type: Types.FloatVector): Multiplication<FloatVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): FloatVectorValue {
            val left = arguments[0] as FloatVectorValue
            val right = arguments[1] as FloatValue
            return FloatVectorValue(FloatArray(this.d) { left[it].value * right.value })
        }
    }

    /**
     * Multiplication of a [DoubleVectorValue] with a [DoubleValue].
     */
    class DoubleVector(type: Types.DoubleVector): Multiplication<DoubleVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleVectorValue {
            val left = arguments[0] as DoubleVectorValue
            val right = arguments[1] as DoubleValue
            return DoubleVectorValue(DoubleArray(this.d) { left[it].value * right.value })
        }
    }
}