package org.vitrivr.cottontail.dbms.functions

import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionRegistry
import org.vitrivr.cottontail.core.queries.functions.math.arithmetics.scalar.*
import org.vitrivr.cottontail.core.queries.functions.math.arithmetics.vector.Sum
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.*
import org.vitrivr.cottontail.core.queries.functions.math.distance.ternary.HyperplaneDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.ternary.WeightedManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.score.FulltextScore

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
    this.register(Addition)
    this.register(Subtraction)
    this.register(Multiplication)
    this.register(Division)
    this.register(Maximum)
    this.register(Minimum)

    /** Vector arithmetics. */
    this.register(org.vitrivr.cottontail.core.queries.functions.math.arithmetics.vector.Addition)
    this.register(org.vitrivr.cottontail.core.queries.functions.math.arithmetics.vector.Subtraction)
    this.register(org.vitrivr.cottontail.core.queries.functions.math.arithmetics.vector.Multiplication)
    this.register(org.vitrivr.cottontail.core.queries.functions.math.arithmetics.vector.Maximum)
    this.register(org.vitrivr.cottontail.core.queries.functions.math.arithmetics.vector.Minimum)
    this.register(Sum)
}

/**
 * Registers default [VectorDistance] functions.
 */
private fun FunctionRegistry.initializeVectorDistance() {
    this.register(ManhattanDistance)
    this.register(EuclideanDistance)
    this.register(SquaredEuclideanDistance)
    this.register(HammingDistance)
    this.register(HaversineDistance)
    this.register(CosineDistance)
    this.register(ChisquaredDistance)
    this.register(InnerProductDistance)
    this.register(HyperplaneDistance)
    this.register(WeightedManhattanDistance)
}
