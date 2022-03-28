package org.vitrivr.cottontail.dbms.functions.math.distance.binary

import jdk.incubator.vector.VectorOperators
import jdk.incubator.vector.VectorSpecies
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.functions.math.VectorDistance
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A [VectorDistance] implementation to calculate the Cosine distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class HammingDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): VectorDistance<T>(type) {

    companion object: FunctionGenerator<DoubleValue> {
        val FUNCTION_NAME = Name.FunctionName("hamming")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Function<DoubleValue> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            if (signature.arguments.any { it != signature.arguments[0] }) throw FunctionNotSupportedException("Function generator ${HaversineDistance.signature} cannot generate function with signature $signature.")
            return when(val type = signature.arguments[0].type) {
                is Types.BooleanVector -> BooleanVector(type)
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
                BooleanVector(Types.BooleanVector(1)).signature,
                DoubleVector(Types.DoubleVector(1)).signature,
                FloatVector(Types.FloatVector(1)).signature,
                LongVector(Types.LongVector(1)).signature,
                IntVector(Types.IntVector(1)).signature
            )
        }
    }

    /** The [Cost] of applying this [HammingDistance]. */
    override val cost: Cost
        get() = (Cost.FLOP + Cost.MEMORY_ACCESS) * this.d

    /**
     * [HammingDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): HammingDistance<DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))

        override fun vectorized(): VectorDistance<DoubleVectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [HammingDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): HammingDistance<FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))

        override fun vectorized(): VectorDistance<FloatVectorValue> {
            return FloatVectorVectorized(type)
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * SIMD Implementation: [HammingDistance] for a [FloatVectorValue].
     */
    class FloatVectorVectorized(type: Types.Vector<FloatVectorValue,*>): HammingDistance<FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val species: VectorSpecies<Float> = jdk.incubator.vector.FloatVector.SPECIES_PREFERRED
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var sum = 0.0

            //Vectorized calculation
            for (i in 0 until species.loopBound(this.d) step species.length()) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(species, probing.data, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(species, query.data, i)
                sum += vp.compare(VectorOperators.NE, vq).trueCount()
            }

            // Scalar calculation for the remaining lanes, since SPECIES.loopBound(this.d) <= this.d
            for (i in species.loopBound(this.d) until this.d) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = FloatVectorVectorized(Types.FloatVector(d))

        override fun vectorized(): VectorDistance<FloatVectorValue> {
            return this
        }
    }

    /**
     * [HammingDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.Vector<LongVectorValue,*>): HammingDistance<LongVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = LongVector(Types.LongVector(d))

        override fun vectorized(): VectorDistance<LongVectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [HammingDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.Vector<IntVectorValue,*>): HammingDistance<IntVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = IntVector(Types.IntVector(d))

        override fun vectorized(): VectorDistance<IntVectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [HammingDistance] for a [IntVectorValue].
     */
    class BooleanVector(type: Types.Vector<BooleanVectorValue,*>): HammingDistance<BooleanVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as BooleanVectorValue
            val query = arguments[1] as BooleanVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = BooleanVector(Types.BooleanVector(d))

        override fun vectorized(): VectorDistance<BooleanVectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }
}