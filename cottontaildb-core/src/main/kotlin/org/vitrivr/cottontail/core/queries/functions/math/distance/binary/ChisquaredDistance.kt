package org.vitrivr.cottontail.core.queries.functions.math.distance.binary

import jdk.incubator.vector.VectorOperators
import jdk.incubator.vector.VectorSpecies
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.*
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.*
import kotlin.math.pow

/**
 * A [VectorDistance] implementation to calculate the Chi^2 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class ChisquaredDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): VectorDistance<T>(type) {

    companion object: FunctionGenerator<DoubleValue> {
        val FUNCTION_NAME = Name.FunctionName.create("chisquared")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Function<DoubleValue> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!" }
            if (signature.arguments.any { it != signature.arguments[0] }) throw FunctionNotSupportedException("Function generator ${HaversineDistance.signature} cannot generate function with signature $signature.")
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

    /** The [Cost] of applying this [ChisquaredDistance]. */
    override val cost: Cost
        get() = (Cost.FLOP * 5.0f + Cost.MEMORY_ACCESS * 4.0f) * this.vectorSize

    /**
     * [ChisquaredDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): ChisquaredDistance<DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]).pow(2)) / (query.data[i] + probing.data[i])
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))
    }

    /**
     * [ChisquaredDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): ChisquaredDistance<FloatVectorValue>(type), VectorisableFunction<DoubleValue> {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]).pow(2)) / (query.data[i] + probing.data[i])
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))

        override fun vectorized() = FloatVectorVectorized(this.type)
    }

    /**
     * SIMD implementation: [ChisquaredDistance] for a [FloatVectorValue]
     */
    class FloatVectorVectorized(type: Types.Vector<FloatVectorValue,*>): EuclideanDistance<FloatVectorValue>(type), VectorisedFunction<DoubleValue> {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            // Changing SPECIES to SPECIES.PREFERRED results in a HUGE performance decrease
            val species: VectorSpecies<Float> = jdk.incubator.vector.FloatVector.SPECIES_PREFERRED
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var vectorSum = jdk.incubator.vector.FloatVector.zero(species)

            //Vectorized calculation
            for (i in 0 until species.loopBound(this.vectorSize) step species.length()) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(species, probing.data, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(species, query.data, i)
                vectorSum = vectorSum.lanewise(VectorOperators.ADD, vp.lanewise(VectorOperators.SUB, vq).pow(2f)
                    .div(vp.lanewise(VectorOperators.ADD, vq)))
            }

            var sum = vectorSum.reduceLanes(VectorOperators.ADD)

            // Scalar calculation for the remaining lanes, since SPECIES.loopBound(this.d) <= this.d
            for (i in species.loopBound(this.vectorSize) until this.vectorSize) {
                sum += ((query.data[i] - probing.data[i]).pow(2)) / (query.data[i] + probing.data[i])
            }

            return DoubleValue(sum)
        }
        override fun copy(d: Int) = FloatVectorVectorized(Types.FloatVector(d))
    }

    /**
     * [ChisquaredDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.Vector<LongVectorValue,*>): ChisquaredDistance<LongVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]).toDouble().pow(2)) / (query.data[i] + probing.data[i])
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = LongVector(Types.LongVector(d))
    }

    /**
     * [ChisquaredDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.Vector<IntVectorValue,*>): ChisquaredDistance<IntVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing =  arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]).toDouble().pow(2)) / (query.data[i] + probing.data[i])
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = IntVector(Types.IntVector(d))
    }
}