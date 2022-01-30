package org.vitrivr.cottontail.functions.math.distance

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.functions.math.distance.binary.*

/**
 * An enumeration of all [Distances] supported by Cottontail DB for proximity based queries.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
enum class Distances(val functionName: Name.FunctionName) {
    L1(ManhattanDistance.FUNCTION_NAME),
    L2(EuclideanDistance.FUNCTION_NAME),
    L2SQUARED(SquaredEuclideanDistance.FUNCTION_NAME),
    HAMMING(HammingDistance.FUNCTION_NAME),
    COSINE(CosineDistance.FUNCTION_NAME),
    CHISQUARED(ChisquaredDistance.FUNCTION_NAME),
    INNERPRODUCT(InnerProductDistance.FUNCTION_NAME),
    HAVERSINE(HaversineDistance.FUNCTION_NAME);
}