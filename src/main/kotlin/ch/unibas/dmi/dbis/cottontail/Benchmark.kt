package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.grpc.CottonDDLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import io.grpc.ManagedChannelBuilder

object Benchmark {

    private val channel = ManagedChannelBuilder.forAddress("10.34.58.76", 1865).usePlaintext().build()
    private val ddlService = CottonDDLGrpc.newBlockingStub(channel)

    @JvmStatic
    fun main(args: Array<String>) {
        prepareEntities()
    }

    /**
     * Prepares entities for LIRE.
     */
    fun prepareEntities() {
        this.ddlService.createEntity(
                CottontailGrpc.CreateEntityMessage.newBuilder()
                        .setEntity(CottontailGrpc.Entity.newBuilder().setName("feature_scalablecolor").setSchema(CottontailGrpc.Schema.newBuilder().setName("cineast")))
                        .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setName("id").setType(CottontailGrpc.Type.STRING).setNullable(false))
                        .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setName("feature").setType(CottontailGrpc.Type.FLOAT_VEC).setLength(64).setNullable(false))
                        .build()
        )

        this.ddlService.createEntity(
                CottontailGrpc.CreateEntityMessage.newBuilder()
                        .setEntity(CottontailGrpc.Entity.newBuilder().setName("feature_cedd").setSchema(CottontailGrpc.Schema.newBuilder().setName("cineast")))
                        .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setName("id").setType(CottontailGrpc.Type.STRING).setNullable(false))
                        .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setName("feature").setType(CottontailGrpc.Type.FLOAT_VEC).setLength(144).setNullable(false))
                        .build()
        )

        this.ddlService.createEntity(
                CottontailGrpc.CreateEntityMessage.newBuilder()
                        .setEntity(CottontailGrpc.Entity.newBuilder().setName("feature_jhist").setSchema(CottontailGrpc.Schema.newBuilder().setName("cineast")))
                        .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setName("id").setType(CottontailGrpc.Type.STRING).setNullable(false))
                        .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setName("feature").setType(CottontailGrpc.Type.FLOAT_VEC).setLength(576).setNullable(false))
                        .build()
        )

        this.ddlService.createEntity(
                CottontailGrpc.CreateEntityMessage.newBuilder()
                        .setEntity(CottontailGrpc.Entity.newBuilder().setName("feature_acc").setSchema(CottontailGrpc.Schema.newBuilder().setName("cineast")))
                        .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setName("id").setType(CottontailGrpc.Type.STRING).setNullable(false))
                        .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setName("feature").setType(CottontailGrpc.Type.FLOAT_VEC).setLength(1024).setNullable(false))
                        .build()
        )
    }
}