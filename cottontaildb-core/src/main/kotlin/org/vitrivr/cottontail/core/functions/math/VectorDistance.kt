package org.vitrivr.cottontail.core.functions.math

import org.vitrivr.cottontail.core.functions.AbstractFunction
import org.vitrivr.cottontail.core.functions.Argument
import org.vitrivr.cottontail.core.functions.Signature
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A binary, [AbstractFunction] used for distance calculations between a query [VectorValue] and other vector values.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class VectorDistance<T : VectorValue<*>>(val name: Name.FunctionName, val type: Types.Vector<T,*>)
    : AbstractFunction<DoubleValue>(Signature.Closed(name, arrayOf(Argument.Typed(type), Argument.Typed(type)), Types.Double)) {

    /** The dimensionality of this [VectorDistance]. */
    val d: Int
        get() = this.type.logicalSize

    /**
     * Creates a copy of this [VectorDistance]. Can be used to create different shapes by choosing a new value for [d].
     *
     * @param d The new dimensionality of the [VectorDistance].
     * @return New [VectorDistance]
     */
    abstract fun copy(d: Int = this.d): VectorDistance<T>
}