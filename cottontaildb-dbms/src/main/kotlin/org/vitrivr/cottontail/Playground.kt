package org.vitrivr.cottontail

import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.values.StringValue
import java.util.*

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */

fun main() {
    val channel = ManagedChannelBuilder.forAddress("localhost", 1865).usePlaintext().build()
    val client = SimpleClient(channel)

    val query = Query("cinest.features_inceptionresnetv2")
        .select("id")
        .disallowParallelism()

    val bruteForce = LinkedList<StringValue>()
    //val index = LinkedList<StringValue>()
    client.query(query).forEach {}

    //client.query(query.disallowIndex()).forEach {
   // }

   // val recall = RankingUtilities.recallAtK(index, bruteForce, 100)
    //println("Recall with index was $recall.")
}