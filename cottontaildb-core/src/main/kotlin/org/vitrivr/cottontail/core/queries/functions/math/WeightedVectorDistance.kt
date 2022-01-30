package org.vitrivr.cottontail.core.queries.functions.math

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A ternary, [AbstractFunction] used for weighted, distance calculations between a query [VectorValue] and other vector values.
 *
 * These type of [AbstractFunction] always accept three of the same [Types.Vector] (query argument, probing argument and weight)
 * and always return a [Types.Double].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class WeightedVectorDistance<T : VectorValue<*>>(val type: Types.Vector<T,*>): Function<DoubleValue> {
    /** The [Types.Vector] accepted by this [WeightedVectorDistance]. */
    abstract val name: Name.FunctionName

    /** The dimensionality of this [WeightedVectorDistance]. */
    val d: Int
        get() = this.type.logicalSize

    /** Signature of a [WeightedVectorDistance] is defined by the argument type it accepts. */
    override val signature: Signature.Closed<DoubleValue>
        get() = Signature.Closed(name, arrayOf(this.type, this.type, this.type), Types.Double)

    /**
     * Creates a copy of this [WeightedVectorDistance].
     *
     * @return Copy of this [WeightedVectorDistance]
     */
    override fun copy(): WeightedVectorDistance<T> = copy(this.d)

    /**
     * Creates reshaped a copy of this [WeightedVectorDistance].
     *
     * @return Copy of this [WeightedVectorDistance]
     */
    abstract fun copy(d: Int): WeightedVectorDistance<T>
}