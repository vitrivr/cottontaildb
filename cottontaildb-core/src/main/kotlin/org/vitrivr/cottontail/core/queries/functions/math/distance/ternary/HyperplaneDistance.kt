package org.vitrivr.cottontail.core.queries.functions.math.distance.ternary

import jdk.incubator.vector.VectorOperators
import jdk.incubator.vector.VectorSpecies
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.*
import kotlin.math.sqrt

/**
 * A [Function] implementation to calculate the distance between a [VectorValue]s and
 * a hyperplane defined by w * x + b = 0, with w,x ∈ ℝ^n and b ∈ ℝ
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class HyperplaneDistance<T: VectorValue<*>>(val type: Types.Vector<T,*>): Function<DoubleValue> {

    /**
     * The [FunctionGenerator] for the [HyperplaneDistance].
     */
    companion object: FunctionGenerator<DoubleValue> {
        val FUNCTION_NAME = Name.FunctionName("hyperplane")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector, Argument.Numeric))

        override fun obtain(signature: Signature.SemiClosed): Function<DoubleValue> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!" }
            return when (val type = signature.arguments[0].type) {
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

    /** The [Signature.Closed] of this [HyperplaneDistance]. */
    override val signature = Signature.Closed(FUNCTION_NAME, arrayOf(this.type, this.type, Types.Double), Types.Double)

    /** The dimensionality of this [HyperplaneDistance]. */
    val d: Int
        get() = this.type.logicalSize

    /** The [Cost] of applying this [HyperplaneDistance]. */
    override val cost: Cost
        get() = ((Cost.FLOP * 4.0f + Cost.MEMORY_ACCESS * 6.0f) * this.d) + Cost.OP_SQRT + Cost.FLOP * 2.0f

    /**
     * [HyperplaneDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.DoubleVector): HyperplaneDistance<DoubleVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            val bias = arguments[2] as DoubleValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until this.d) {
                dotp += probing.data[i] * query.data[i]
                norm += query.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / sqrt(norm))
        }
    }

    /**
     * [HyperplaneDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.FloatVector): HyperplaneDistance<FloatVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            val bias = arguments[2] as FloatValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until this.d) {
                dotp += probing.data[i] * query.data[i]
                norm += query.data[i] * query.data[i]
            }
            return DoubleValue((dotp + bias.value) / sqrt(norm))
        }
    }

    /**
     * SIMD Implementation: [HyperplaneDistance] for a [FloatVectorValue].
     */
    class FloatVectorVectorized(type: Types.FloatVector): HyperplaneDistance<FloatVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val species: VectorSpecies<Float> = jdk.incubator.vector.FloatVector.SPECIES_PREFERRED
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            val bias = arguments[2] as FloatValue
            var vectorDotp = jdk.incubator.vector.FloatVector.zero(species)
            var vectorNorm = jdk.incubator.vector.FloatVector.zero(species)

            for (i in 0 until species.loopBound(this.d) step species.length()) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(species, probing.data, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(species, query.data, i)

                vectorDotp = vp.fma(vq, vectorDotp)
                vectorNorm = vectorNorm.lanewise(VectorOperators.ADD, vq.pow(2f))
            }

            var dotp = vectorDotp.reduceLanes(VectorOperators.ADD)
            var norm = vectorNorm.reduceLanes(VectorOperators.ADD)

            for (i in species.loopBound(this.d) until this.d) {
                dotp += probing.data[i] * query.data[i]
                norm += query.data[i] * query.data[i]
            }

            return DoubleValue((dotp + bias.value) / sqrt(norm))
        }
    }

    /**
     * [HyperplaneDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.LongVector): HyperplaneDistance<LongVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            val bias = arguments[2] as DoubleValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until this.d) {
                dotp += probing.data[i] * query.data[i]
                norm += query.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / norm)
        }
    }

    /**
     * [HyperplaneDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.IntVector): HyperplaneDistance<IntVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            val bias = arguments[2] as DoubleValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until this.d) {
                dotp += probing.data[i] * query.data[i]
                norm += query.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / norm)
        }
    }
}
