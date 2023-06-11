package org.vitrivr.cottontail.client.language.basics

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An enumeration of all [Distances] supported by Cottontail DB for NNS.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@Serializable
enum class Distances(val functionName: String) {
    L1("manhattan"),
    MANHATTAN("manhattan"),
    L2("euclidean"),
    EUCLIDEAN("euclidean"),
    SQUAREDEUCLIDEAN("squaredeuclidean"),
    L2SQUARED("squaredeuclidean"),
    HAMMING("hamming"),
    COSINE("cosine"),
    CHI2("chisquared"),
    CHISQUARED("chisquared"),
    IP("innerproduct"),
    INNERPRODUCT("innerproduct"),
    DOTP("innerproduct"),
    HAVERSINE("haversine");

    /**
     * Converts this [Distances] to a correspnding [CottontailGrpc.FunctionName]
     *
     * @return [CottontailGrpc.FunctionName] for this [Distances].
     */
    fun toGrpc() = CottontailGrpc.FunctionName.newBuilder().setName(this.functionName)
}