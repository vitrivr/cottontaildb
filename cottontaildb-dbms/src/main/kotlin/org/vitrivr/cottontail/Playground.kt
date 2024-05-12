package org.vitrivr.cottontail

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.utilities.math.ranking.RankingUtilities
import java.util.LinkedList

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */

fun main() {
    val channel = ManagedChannelBuilder.forAddress("localhost", 1865).usePlaintext().build()
    val client = SimpleClient(channel)

    val query = Query("cineast-lsc2.features_inceptionresnetv2")
        .select("id")
        .distance("feature", FloatVectorValueGenerator.random(1536), Distances.EUCLIDEAN, "score")
        .order("score", Direction.ASC)
        .limit(1000)

    val bruteForce = LinkedList<StringValue>()
    val index = LinkedList<StringValue>()
    client.query(query).forEach {
        index.add(it["id"] as StringValue)
    }
    client.query(query.disallowIndex()).forEach {
        bruteForce.add(it["id"] as StringValue)
    }

    val recall = RankingUtilities.recallAtK(index, bruteForce, 100)
    println("Recall with index was $recall.")
}