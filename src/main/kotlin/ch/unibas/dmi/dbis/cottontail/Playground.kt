package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.grpc.CottonDDLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDMLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDQLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.server.grpc.helper.DataHelper
import ch.unibas.dmi.dbis.cottontail.utilities.VectorUtility

import com.google.gson.GsonBuilder
import io.grpc.ManagedChannelBuilder

import java.nio.file.Files
import java.nio.file.Paths


object Playground {


    val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()

    val dqlService =  CottonDQLGrpc.newBlockingStub(channel)
    val ddlService =  CottonDDLGrpc.newBlockingStub(channel)
    val dmlService =  CottonDMLGrpc.newBlockingStub(channel)

    val schema = CottontailGrpc.Schema.newBuilder().setName("test").build()
    val entity = CottontailGrpc.Entity.newBuilder()
            .setSchema(schema)
            .setName("surf")
            .build()


    @JvmStatic
    fun main(args: Array<String>) {



        this.ddlService.createIndex(
                CottontailGrpc.CreateIndexMessage.newBuilder().addColumns("feature").setIndex(
                    CottontailGrpc.Index.newBuilder()
                            .setEntity(CottontailGrpc.Entity.newBuilder().setName("features_audiotranscription").setSchema(CottontailGrpc.Schema.newBuilder().setName("cineast").build()))
                            .setType(CottontailGrpc.Index.IndexType.LUCENE)
                            .setName("feature_index")
                            .build()
                ).build()
        )

    }
}