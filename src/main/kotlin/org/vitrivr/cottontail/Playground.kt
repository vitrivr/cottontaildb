package org.vitrivr.cottontail

import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottonDMLGrpc
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.values.IntVectorValue
import java.nio.file.Files
import java.nio.file.Paths


object Playground {


    val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()

    val dqlService = CottonDQLGrpc.newBlockingStub(channel)
    val ddlService = CottonDDLGrpc.newBlockingStub(channel)
    val dmlService = CottonDMLGrpc.newBlockingStub(channel)

    val schema = CottontailGrpc.Schema.newBuilder().setName("cottontail").build()
    val entity = CottontailGrpc.Entity.newBuilder()
            .setSchema(schema)
            .setName("tab4")
            .build()


    @JvmStatic
    fun main(args: Array<String>) {
        this.executeKnn()
    }


    private fun executeUpdate() {
        val truncateMessage = CottontailGrpc.UpdateMessage.newBuilder()
                .setFrom(CottontailGrpc.From.newBuilder().setEntity(entity))
                .setWhere(CottontailGrpc.Where.newBuilder().setCompound(
                        CottontailGrpc.CompoundBooleanPredicate.newBuilder()
                                .setAleft(CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                        .setOp(CottontailGrpc.AtomicLiteralBooleanPredicate.Operator.EQUAL)
                                        .setAttribute("objectid")
                                        .addData(CottontailGrpc.Data.newBuilder().setStringData("v_00071")))
                                .setAright(CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                        .setOp(CottontailGrpc.AtomicLiteralBooleanPredicate.Operator.EQUAL)
                                        .setAttribute("key")
                                        .addData(CottontailGrpc.Data.newBuilder().setStringData("duration")))

                ))
                .setTuple(CottontailGrpc.Tuple.newBuilder().putData("value", CottontailGrpc.Data.newBuilder().setStringData("this is a test!").build()))
        val results = this.dmlService.update(truncateMessage.build())
        results.forEach {
            it.resultsList.forEach {
                println(it)
            }
        }
    }

    private fun executeDelete() {
        val truncateMessage = CottontailGrpc.DeleteMessage.newBuilder()
                .setFrom(CottontailGrpc.From.newBuilder().setEntity(entity))
                .setWhere(CottontailGrpc.Where.newBuilder().setAtomic(
                        CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                .setOp(CottontailGrpc.AtomicLiteralBooleanPredicate.Operator.EQUAL)
                                .setAttribute("objectid")
                                .addData(CottontailGrpc.Data.newBuilder().setStringData("v_00093"))
                ))
        val results = this.dmlService.delete(truncateMessage.build())
        results.forEach {
            it.resultsList.forEach {
                println(it)
            }
        }
    }


    private fun executeIn() {
        Files.newInputStream(Paths.get("/Users/rgasser/Downloads/in-like.proto")).use {
            val query = CottontailGrpc.QueryMessage.parseFrom(it)
            val results = this.dqlService.query(query)
            results.forEach {
                it.resultsList.forEach {
                    println(it)
                }
            }
        }
    }


    private fun executeKnn() {
        val vector = IntVectorValue.random(2).let {
            CottontailGrpc.Vector.newBuilder().setIntVector(CottontailGrpc.IntVector.newBuilder().addAllVector(it.data.asIterable()))
        }
        val query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(CottontailGrpc.From.newBuilder().setEntity(entity))
                        .setKnn(CottontailGrpc.Knn.newBuilder()
                                .addQuery(vector)
                                .setDistance(CottontailGrpc.Knn.Distance.L2)
                                .setK(5)
                                .setAttribute("col27")
                                .setHint(CottontailGrpc.KnnHint.newBuilder().setNoIndexHint(CottontailGrpc.KnnHint.NoIndexKnnHint.getDefaultInstance()))
                        )
                        .setProjection(CottontailGrpc.Projection.newBuilder().putAttributes("col26", "ctid").putAttributes("distance", "dist"))
                        .setLimit(5)
        )
        val results = this.dqlService.query(query.build())
        results.forEach {
            it.resultsList.forEach {
                println(it)
            }
        }
    }
}