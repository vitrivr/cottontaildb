package org.vitrivr.cottontail

import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc.Expression
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@OptIn(ExperimentalTime::class)
fun main(args: Array<String>) {
    val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext()
    val client = SimpleClient(channel.build())
    val queryVector = FloatVectorValueGenerator.random(2048)
    val queryVectorMessage = CottontailGrpc.Vector.newBuilder().setFloatVector(CottontailGrpc.FloatVector.newBuilder().addAllVector(queryVector.data.asIterable()))

    val query = CottontailGrpc.Query.newBuilder().setFrom(
        CottontailGrpc.From.newBuilder().setScan(
            CottontailGrpc.Scan.newBuilder().setEntity(
                CottontailGrpc.EntityName.newBuilder().setName("features_conceptmasksade20k")
                    .setSchema(CottontailGrpc.SchemaName.newBuilder().setName("cineast"))))
    ).setProjection(
        CottontailGrpc.Projection.newBuilder()
            .addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setExpression(Expression.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("id"))))
            .addElements(
                CottontailGrpc.Projection.ProjectionElement.newBuilder().setExpression(/* Distance function */
                    Expression.newBuilder().setFunction(CottontailGrpc.Function.newBuilder().setName(CottontailGrpc.FunctionName.newBuilder().setName("euclidean")).addArguments(
                        Expression.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("feature"))
                    ).addArguments(
                        Expression.newBuilder().setLiteral(CottontailGrpc.Literal.newBuilder().setVectorData(queryVectorMessage))
                    ))
                ).setAlias(CottontailGrpc.ColumnName.newBuilder().setName("distance"))
            )
    ).setOrder(
        CottontailGrpc.Order.newBuilder().addComponents(CottontailGrpc.Order.Component.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("distance")))
    ).setLimit(500)

    /* Execute with index. */
    val timedWithIndex = measureTimedValue {
        val results = client.query(CottontailGrpc.QueryMessage.newBuilder()
            .setQuery(query)
            .setMetadata(CottontailGrpc.Metadata.newBuilder()
            .addHint(CottontailGrpc.Hint.newBuilder().setParallelIndexHint(CottontailGrpc.Hint.NoParallelHint.getDefaultInstance())))
            .build())
        TabulationUtilities.tabulate(results)
    }
    println("Results generated (index) in ${timedWithIndex.duration}.")
    //println(timedWithIndex.value)

    /* Execute without index. */
    val timedWithoutIndex = measureTimedValue {
        val results = client.query(CottontailGrpc.QueryMessage.newBuilder()
            .setQuery(query)
            .setMetadata(CottontailGrpc.Metadata.newBuilder()
                .addHint(CottontailGrpc.Hint.newBuilder().setParallelIndexHint(CottontailGrpc.Hint.NoParallelHint.getDefaultInstance()))
                .addHint(CottontailGrpc.Hint.newBuilder().setNoIndexHint(CottontailGrpc.Hint.NoIndexHint.getDefaultInstance()))
            ).build())
        TabulationUtilities.tabulate(results)
    }
    println("Results generated (no index) in ${timedWithoutIndex.duration}.")

    //println(timedWithoutIndex.value)
}