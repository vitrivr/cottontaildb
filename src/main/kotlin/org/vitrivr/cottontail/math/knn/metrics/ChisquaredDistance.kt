package org.vitrivr.cottontail.math.knn.metrics

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

object ChisquaredDistance : DistanceKernel {
    override val cost: Double
        get() = 1.0

    override fun invoke(a: VectorValue<*>, b: VectorValue<*>): DoubleValue = (((b - a).pow(2)) / (b + a)).sum().asDouble()

    override fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): DoubleValue = ((((b - a).pow(2)) / (b + a)) * weights).sum().asDouble()
}