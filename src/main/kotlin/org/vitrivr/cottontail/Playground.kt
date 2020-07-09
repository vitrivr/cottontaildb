package org.vitrivr.cottontail

import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottonDMLGrpc
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc


object Playground {


    val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()

    val dqlService = CottonDQLGrpc.newBlockingStub(channel)
    val ddlService = CottonDDLGrpc.newBlockingStub(channel)
    val dmlService = CottonDMLGrpc.newBlockingStub(channel)

    val schema = CottontailGrpc.Schema.newBuilder().setName("test").build()
    val entity = CottontailGrpc.Entity.newBuilder()
            .setSchema(schema)
            .setName("surf")
            .build()


    @JvmStatic
    fun main(args: Array<String>) {


        this.ddlService.createIndex(
                CottontailGrpc.IndexDefinition.newBuilder().addColumns("feature").setIndex(
                        CottontailGrpc.Index.newBuilder()
                                .setEntity(CottontailGrpc.Entity.newBuilder().setName("features_audiotranscription").setSchema(CottontailGrpc.Schema.newBuilder().setName("cineast").build()))
                                .setType(CottontailGrpc.IndexType.LUCENE)
                                .setName("feature_index")
                                .build()
                ).build()
        )

    }
}