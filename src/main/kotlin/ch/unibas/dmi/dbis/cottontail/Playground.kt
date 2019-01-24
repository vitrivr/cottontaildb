package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.schema.*
import ch.unibas.dmi.dbis.cottontail.knn.KnnContainer
import ch.unibas.dmi.dbis.cottontail.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.utilities.VectorUtility

import com.google.gson.GsonBuilder
import kotlinx.serialization.json.JSON

import java.io.IOException
import java.lang.IllegalArgumentException
import java.nio.file.Files
import java.nio.file.Paths

import kotlin.concurrent.thread

object Playground {

    @JvmStatic
    fun main(args: Array<String>) {
        val path = args[0]
        Files.newBufferedReader(Paths.get(path)).use { reader ->
            val config = JSON.parse(Config.serializer(), reader.readText())
                loadAndPersist(config)
        }
    }

    fun loadAndRead(config: Config) {
        try {
            val schema = Schema("cottontail", config)
            val entity = schema.get("test")
            val tx = entity.Tx(readonly = true)

            val block = {
                val start = System.currentTimeMillis()
                val container = KnnContainer(10000,VectorUtility.randomFloatVector(2048), Distance.L2)
                var error = 0
                tx.forEachColumn({ l: Long, floats: FloatArray -> try {container.add(l,floats)} catch(e: IllegalArgumentException) {error += 1} }, ColumnDef.withAttributes("feature","FLOAT_VEC", 2048))
                println("${Thread.currentThread().name}: kNN for d=${container.distance::class.simpleName} L2n=${container.knn.size}, s=${tx.count()}, e=$error vectors took ${System.currentTimeMillis() - start} ms")
            }

            for (i in 1..1) {
                thread(start = true, block = block)
            }


        } catch (e: IOException) {
            e.printStackTrace()
        }

    }


    fun loadAndPersist(config: Config) {
        val schema = Schema.create("cottontail", config)
        schema.createEntity("test", ColumnDef("id", StringColumnType()), ColumnDef("feature", FloatArrayColumnType(),2048))
        val entity = schema.get("test")
        for (i in 1..39) {
            val path = Paths.get("/Users/gassra02/Downloads/data/features_conceptmasksade20k_$i.json")
            if (Files.exists(path)) {
                val tx = entity.Tx(readonly = false)
                tx.begin {
                    Files.newBufferedReader(path).use { reader ->
                        val gson = GsonBuilder().create()
                        val features = gson.fromJson(reader, Array<Feature>::class.java)
                        val start = System.currentTimeMillis()
                        for (f in features) {
                            tx.insert(mapOf(
                                    Pair(ColumnDef.withAttributes("id", "STRING"), f.id),
                                    Pair(ColumnDef.withAttributes("feature", "FLOAT_VEC", 2048), f.feature)
                            ))
                        }
                        println(String.format("Writing %d vectors took %d ms", features.size, System.currentTimeMillis() - start))
                    }
                    true
                }

            }
        }
    }
}