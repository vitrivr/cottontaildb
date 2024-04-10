package org.vitrivr.cottontail

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */

fun main(args: Array<String>) {

    val channel: ManagedChannel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865)
        .enableFullStreamDecompression()
        .usePlaintext()
        .build()

    val client = SimpleClient(channel)


    val query = Query(Name.EntityName.create("vitrivr", "descriptor_clip"))
        .select("retrievableId")
        .distance("descriptor", FloatVectorValueGenerator.zero(512), Distances.L2, "distance")
        .order("distance", Direction.ASC)
        .limit(1000)


    val results = client.query(query).forEach {
        println(it)
    }
}

