package org.vitrivr.cottontail.core.queries.functions.math

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A binary, [Function] used for distance calculations between a query [VectorValue] and other vector values.
 *
 * These type of [Function] always accept two of the same [Types.Vector] (query argument and probing argument)
 * and always return a [Types.Double].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class VectorDistance<T: VectorValue<*>>(val type: Types.Vector<T,*>): Function<DoubleValue> {

    /** The [Types.Vector] accepted by this [VectorDistance]. */
    abstract val name: Name.FunctionName

    /** The dimensionality of this [VectorDistance]. */
    val d: Int
        get() = this.type.logicalSize

    /** Signature of a [VectorDistance] is defined by the argument type it accepts. */
    override val signature: Signature.Closed<DoubleValue>
        get() = Signature.Closed(name, arrayOf(this.type, this.type), Types.Double)

    /**
     * Creates a copy of this [VectorDistance].
     *
     * @return Copy of this [VectorDistance]
     */
    override fun copy(): VectorDistance<T> = copy(this.d)

    /**
     * Creates a reshaped copy of this [VectorDistance].
     *
     * @return Copy of this [VectorDistance]
     */
    abstract fun copy(d: Int): VectorDistance<T>

    /**
     * Returns the vectorized Version of the [VectorDistance].
     *
     * @return Vectorized [VectorDistance]
     */
    abstract fun vectorized(): VectorDistance<T>
}