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
        loadAndRead()
    }

    fun loadAndRead() {

        val vector = CottontailGrpc.Vector.newBuilder().setDoubleVector(CottontailGrpc.DoubleVector.newBuilder().addAllVector(VectorUtility.randomDoubleVector(512).asIterable()))
        val queryKnn = CottontailGrpc.Query.newBuilder()
                .setFrom(CottontailGrpc.From.newBuilder().setEntity(entity).build())
                .setKnn(CottontailGrpc.Knn.newBuilder().setAttribute("feature").setDistance(CottontailGrpc.Knn.Distance.L2).setK(10000).setQuery(vector))
                .setProjection(CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.SELECT).addAttributes("id").build())


        val inPredicate = CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                .setAttribute("id")
                .setOp(CottontailGrpc.AtomicLiteralBooleanPredicate.Operator.IN)
                .addData(DataHelper.toData("v_03948_2"))
                .addData(DataHelper.toData("v_03948_4"))
                .addData(DataHelper.toData("test"))

        val queryWhere = CottontailGrpc.Query.newBuilder()
                .setFrom(CottontailGrpc.From.newBuilder().setEntity(entity).build())
                .setWhere(CottontailGrpc.Where.newBuilder().setAtomic(inPredicate))
                .setProjection(CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.SELECT).addAttributes("id").addAttributes("feature").build())




        val results = this.dqlService.query(CottontailGrpc.QueryMessage.newBuilder().setQuery(queryWhere).build()).forEach {
            it.queryId
        }
    }

    /**
     *
     */
    fun loadAndPersist() {

        val idColumn = CottontailGrpc.ColumnDefinition.newBuilder().setName("id").setType(CottontailGrpc.Type.STRING).setLength(-1).build()
        val featureColumn = CottontailGrpc.ColumnDefinition.newBuilder().setName("feature").setType(CottontailGrpc.Type.FLOAT_VEC).setLength(512).build()

        /* Create schema and entity. */
        ddlService.createSchema(schema)
        ddlService.createEntity(CottontailGrpc.CreateEntityMessage.newBuilder().setEntity(entity).addColumns(idColumn).addColumns(featureColumn).build())


        for (i in 1..76) {
            val path = Paths.get("/Volumes/Data (Mac)/Extracted/features_surfmf25k512_$i.json")
            if (Files.exists(path)) {
                Files.newBufferedReader(path).use { reader ->
                    val gson = GsonBuilder().create()
                    val features = gson.fromJson(reader, Array<Feature>::class.java)
                    val start = System.currentTimeMillis()
                    try {
                        val batchSize = 300
                        for (a in 0 until features.size/batchSize) {
                            val insert = CottontailGrpc.InsertMessage.newBuilder().setEntity(entity)
                            for (b in a*batchSize until Math.min(features.size, a*batchSize+batchSize)) {
                                val feature = features[b]
                                insert.addTuple(CottontailGrpc.Tuple.newBuilder().putData("id", DataHelper.toData(feature.id)).putData("feature", DataHelper.toData(feature.feature)))
                            }
                            dmlService.insert(insert.build())
                        }

                        println("Writing ${features.size} vectors took ${System.currentTimeMillis() - start} ms.")
                    } catch(e: DatabaseException) {
                        println("Error persisting data fil file $path: ${e.message}.")
                    }
                }
            }
        }
    }
}