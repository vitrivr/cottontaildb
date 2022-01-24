package org.vitrivr.cottontail.functions

import org.vitrivr.cottontail.core.functions.Function
import org.vitrivr.cottontail.core.functions.FunctionRegistry
import org.vitrivr.cottontail.core.functions.math.VectorDistance
import org.vitrivr.cottontail.functions.math.distance.binary.*
import org.vitrivr.cottontail.functions.math.distance.other.HyperplaneDistance
import org.vitrivr.cottontail.functions.math.score.FulltextScore

/**
 * Registers default [Function]s.
 */
fun FunctionRegistry.initialize() {
    this.register(FulltextScore)
    this.initializeArithmetics()
    this.initializeVectorDistance()
}

/**
 * Registers default arithmetics functions.
 */
private fun FunctionRegistry.initializeArithmetics() {
    this.register(org.vitrivr.cottontail.functions.math.arithmetics.scalar.Maximum)
    this.register(org.vitrivr.cottontail.functions.math.arithmetics.scalar.Minimum)
    this.register(org.vitrivr.cottontail.functions.math.arithmetics.vector.Maximum)
    this.register(org.vitrivr.cottontail.functions.math.arithmetics.vector.Maximum)
}

/**
 * Registers default [VectorDistance] functions.
 */
private fun FunctionRegistry.initializeVectorDistance() {
    this.register(ManhattanDistance.Generator)
    this.register(EuclideanDistance.Generator)
    this.register(SquaredEuclideanDistance.Generator)
    this.register(HammingDistance.Generator)
    this.register(HaversineDistance.Generator)
    this.register(CosineDistance.Generator)
    this.register(ChisquaredDistance.Generator)
    this.register(InnerProductDistance.Generator)
    this.register(HyperplaneDistance.Generator)
}