package org.vitrivr.cottontail

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.dql.Query

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */

/** The [ManagedChannel] used to connect to Cottontail DB. */
fun main(args: Array<String>) {
    val channel: ManagedChannel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865)
        .enableFullStreamDecompression()
        .usePlaintext()
        .build()

    /** The [SimpleClient] used to access Cottontail DB. */
    val client = SimpleClient(channel)

    val query1 = Query().sample("cineast.features_visualtextcoembedding", 0.1f).select("feature").limit(1)
    val feature = client.query(query1).next().asFloatVector("feature")!!

    val query2 = Query("cineast.features_visualtextcoembedding").select("id")
        .distance("feature", feature, Distances.L2, "distance")
        .order("distance", Direction.ASC)
        .limit(100L)

    val results = client.query(query2)
    results.forEach { println(it) }
}
