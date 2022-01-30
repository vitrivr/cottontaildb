package org.vitrivr.cottontail.functions.math.arithmetics.vector

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue
import kotlin.math.max

/**
 * A function that compares two vectors and returns a vector that contains the element-wise maximum of both.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Maximum<T : VectorValue<*>>(val type: Types.Vector<T,*>): Function<T> {

    companion object : FunctionGenerator<VectorValue<*>> {
        private val FUNCTION_NAME = Name.FunctionName("vmax")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Maximum<*> {
            check(this.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${this.signature}. This is a programmer's error!"  }
            return when(val type = signature.arguments[0].type) {
                is Types.DoubleVector -> DoubleVector(type)
                is Types.FloatVector -> FloatVector(type)
                is Types.LongVector -> LongVector(type)
                is Types.IntVector -> IntVector(type)
                else -> throw FunctionNotSupportedException("Function generator ${this.signature} cannot generate function with signature $signature.")
            }
        }

        override fun resolve(signature: Signature.Open): List<Signature.Closed<*>> {
            if (this.signature != signature) throw FunctionNotSupportedException("Function generator ${this.signature} cannot generate function with signature $signature.")
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
    override val signature = Signature.Closed(FUNCTION_NAME, arrayOf(this.type, this.type), this.type)

    /** The dimensionality of this [Minimum]. */
    val d: Int
        get() = this.type.logicalSize

    /**
     * (Element-wise) [Maximum] for a [IntVectorValue].
     */
    class IntVector(type: Types.IntVector): Maximum<IntVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): IntVectorValue {
            val left = arguments[0] as IntVectorValue
            val right = arguments[1] as IntVectorValue
            return IntVectorValue(IntArray(this.d) {
                max(left[it].value, right[it].value)
            })
        }
    }

    /**
     * (Element-wise) [Maximum] for a [LongVectorValue].
     */
    class LongVector(type: Types.LongVector): Maximum<LongVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): LongVectorValue {
            val left = arguments[0] as LongVectorValue
            val right = arguments[1] as LongVectorValue
            return LongVectorValue(LongArray(this.d) {
                max(left[it].value, right[it].value)
            })
        }
    }

    /**
     * (Element-wise) [Maximum] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.FloatVector): Maximum<FloatVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): FloatVectorValue {
            val left = arguments[0] as FloatVectorValue
            val right = arguments[1] as FloatVectorValue
            return FloatVectorValue(FloatArray(this.d) {
                max(left[it].value, right[it].value)
            })
        }
    }

    /**
     * (Element-wise) [Maximum] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.DoubleVector): Maximum<DoubleVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleVectorValue {
            val left = arguments[0] as DoubleVectorValue
            val right = arguments[1] as DoubleVectorValue
            return DoubleVectorValue(DoubleArray(this.d) {
                max(left[it].value, right[it].value)
            })
        }
    }
}