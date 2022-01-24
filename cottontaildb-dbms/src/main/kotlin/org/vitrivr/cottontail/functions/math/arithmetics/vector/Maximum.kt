package org.vitrivr.cottontail.functions.math.arithmetics.vector

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.functions.AbstractFunction
import org.vitrivr.cottontail.core.functions.AbstractFunctionGenerator
import org.vitrivr.cottontail.core.functions.Argument
import org.vitrivr.cottontail.core.functions.Signature
import org.vitrivr.cottontail.core.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import kotlin.math.max

/**
 * A function that compares two vectors and returns a vector that contains the element-wise maximum of both.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Maximum<T : VectorValue<*>>(type: Types<T>): AbstractFunction<T>(Signature.Closed(FUNCTION_NAME, arrayOf(Argument.Typed(type), Argument.Typed(type)), type)) {

    companion object : AbstractFunctionGenerator<VectorValue<*>>() {
        private val FUNCTION_NAME = Name.FunctionName("vmax")

        override val signature: Signature.Open<out VectorValue<*>>
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun generateInternal(dst: Signature.Closed<*>): Maximum<*> =
            when (val type = dst.arguments[0].type) {
                is Types.DoubleVector -> DoubleVector(type.logicalSize)
                is Types.FloatVector -> FloatVector(type.logicalSize)
                is Types.IntVector -> IntVector(type.logicalSize)
                is Types.LongVector -> LongVector(type.logicalSize)
                else -> throw FunctionNotSupportedException("Function generator signature $signature does not support destination signature (dst = $dst).")
            }
    }

    /**
     * (Element-wise) [Maximum] for a [IntVectorValue].
     */
    class IntVector(private val size: Int) : Maximum<IntVectorValue>(Types.IntVector(size)) {
        override val cost = 1.0f
        override fun invoke(): IntVectorValue {
            val left = this.arguments[0] as IntVectorValue
            val right = this.arguments[1] as IntVectorValue
            return IntVectorValue(IntArray(this.size) {
                max(left[it].value, right[it].value)
            })
        }
    }

    /**
     * (Element-wise) [Maximum] for a [LongVectorValue].
     */
    class LongVector(private val size: Int) : Maximum<LongVectorValue>(Types.LongVector(size)) {
        override val cost = 1.0f
        override fun invoke(): LongVectorValue {
            val left = this.arguments[0] as LongVectorValue
            val right = this.arguments[1] as LongVectorValue
            return LongVectorValue(LongArray(this.size) {
                max(left[it].value, right[it].value)
            })
        }
    }

    /**
     * (Element-wise) [Maximum] for a [FloatVectorValue].
     */
    class FloatVector(private val size: Int) : Maximum<FloatVectorValue>(Types.FloatVector(size)) {
        override val cost = 1.0f
        override fun invoke(): FloatVectorValue {
            val left = this.arguments[0] as FloatVectorValue
            val right = this.arguments[1] as FloatVectorValue
            return FloatVectorValue(FloatArray(this.size) {
                max(left[it].value, right[it].value)
            })
        }
    }

    /**
     * (Element-wise) [Maximum] for a [DoubleVectorValue].
     */
    class DoubleVector(private val size: Int) : Maximum<DoubleVectorValue>(Types.DoubleVector(size)) {
        override val cost = 1.0f
        override fun invoke(): DoubleVectorValue {
            val left = this.arguments[0] as DoubleVectorValue
            val right = this.arguments[1] as DoubleVectorValue
            return DoubleVectorValue(DoubleArray(this.size) {
                max(left[it].value, right[it].value)
            })
        }
    }
}