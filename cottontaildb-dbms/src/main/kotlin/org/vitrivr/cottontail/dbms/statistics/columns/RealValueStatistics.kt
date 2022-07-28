package org.vitrivr.cottontail.dbms.statistics.columns

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.RealValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A [ValueStatistics] implementation for [RealValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class RealValueStatistics<T: RealValue<*>>(type: Types<T>): AbstractValueStatistics<T>(type) {
    companion object {
        const val MIN_KEY = "min"
        const val MAX_KEY = "max"
        const val SUM_KEY = "sum"
        const val MEAN_KEY = "mean"
    }

    /** Minimum value seen by this [RealValueStatistics]. */
    abstract val min: T

    /** Minimum value seen by this [RealValueStatistics]. */
    abstract val max: T

    /** Sum of all values seen by this [RealValueStatistics]. */
    abstract val sum: DoubleValue

    /** The arithmetic mean for the values seen by this [RealValueStatistics]. */
    val mean: DoubleValue
        get() =  DoubleValue(this.sum.value / this.numberOfNonNullEntries)

    /**
     * Creates a descriptive map of this [RealValueStatistics].
     *
     * @return Descriptive map of this [RealValueStatistics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        MIN_KEY to this.min.value.toString(),
        MAX_KEY to this.max.value.toString(),
        SUM_KEY to this.sum.value.toString(),
        MEAN_KEY to this.mean.value.toString()
    )

    /**
     * Estimates [Selectivity] for a [BooleanPredicate.Atomic].
     *
     * @param predicate [BooleanPredicate.Atomic] to estimate [Selectivity] for.
     * @return [Selectivity]
     */
    context(BindingContext,Record)    override fun estimateSelectivity(predicate: BooleanPredicate.Atomic): Selectivity {
        val op = predicate.operator
        if (op.left is org.vitrivr.cottontail.core.queries.binding.Binding.Column && op.left.type == this.type) {
            when (op) {
                is ComparisonOperator.Binary -> {
                    val left = op.left
                    val right = op.right
                    if (op.right is org.vitrivr.cottontail.core.queries.binding.Binding.Literal) {
                        return this.estimateBinarySelectivity(op, right.getValue() as T)
                    } else if (op.left is org.vitrivr.cottontail.core.queries.binding.Binding.Literal){
                        return this.estimateBinarySelectivity(op, left.getValue() as T)
                    }
                }
                is ComparisonOperator.Between -> {
                    val lower = op.rightLower
                    val upper = op.rightUpper
                    return this.estimateBetweenSelectivity(lower.getValue() as T, upper.getValue() as T)
                }
                else -> { /* No op. */ }
            }
        }
        return super.estimateSelectivity(predicate)
    }

    /**
     * Estimates [Selectivity] for a [ComparisonOperator.Binary] and a fixed [IntValue].
     *
     * @param op [ComparisonOperator.Binary] to estimate selectivity for.
     * @param value [IntValue] to estimate selectivity for.
     * @return [Selectivity]
     */
    private fun estimateBinarySelectivity(op: ComparisonOperator.Binary, value: T): Selectivity {
        val range = (this.max - this.min).asFloat().value + 1.0f
        when(op) {
            is ComparisonOperator.Binary.Equal -> {
                if (value > this.max || value < this.min) return Selectivity.NOTHING
                return Selectivity(1.0f / range) /* Assuming equal distribution. */
            }
            is ComparisonOperator.Binary.Greater -> {
                if (value < this.min) return Selectivity.ALL
                if (value >= this.max) return Selectivity.NOTHING
                return Selectivity((this.max - value).asFloat().value / range) /* Assuming equal distribution. */
            }
            is ComparisonOperator.Binary.GreaterEqual -> {
                if (value <= this.min) return Selectivity.ALL
                if (value >= this.max) return Selectivity.NOTHING
                return Selectivity((this.max - value).asFloat().value / range) /* Assuming equal distribution. */
            }
            is ComparisonOperator.Binary.Less -> {
                if (value > this.max) return Selectivity.ALL
                if (value <= this.min) return Selectivity.NOTHING
                return Selectivity((value - this.min).asFloat().value / range) /* Assuming equal distribution. */
            }
            is ComparisonOperator.Binary.LessEqual -> {
                if (value >= this.max) return Selectivity.ALL
                if (value <= this.min) return Selectivity.NOTHING
                return Selectivity((value - this.min).asFloat().value / range) /* Assuming equal distribution. */
            }
            else -> return Selectivity.DEFAULT
        }
    }

    /**
     * Estimates [Selectivity] for a [ComparisonOperator.Binary] and a fixed [IntValue].
     *
     * @param lower Lower value to estimate selectivity for.
     * @param upper Upper value to estimate selectivity for.
     * @return [Selectivity]
     */
    private fun estimateBetweenSelectivity(lower: T, upper: T): Selectivity {
        if (lower > this.max || upper < this.min) return Selectivity.NOTHING
        val range = (this.max - this.min).asFloat().value + 1.0f
        return Selectivity((upper - lower).asFloat().value / range) /* Assuming equal distribution. */
    }
}