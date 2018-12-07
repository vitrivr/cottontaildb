package ch.unibas.dmis.dbis.cottontail

import ch.unibas.dmis.dbis.cottontail.config.Config
import ch.unibas.dmis.dbis.cottontail.database.definition.FloatArrayColumnType
import ch.unibas.dmis.dbis.cottontail.database.definition.StringColumnType
import ch.unibas.dmis.dbis.cottontail.database.general.AccessorMode
import ch.unibas.dmis.dbis.cottontail.database.schema.Schema
import ch.unibas.dmis.dbis.cottontail.knn.KnnContainer
import ch.unibas.dmis.dbis.cottontail.knn.metrics.Distance
import ch.unibas.dmis.dbis.cottontail.model.DatabaseException
import com.google.gson.GsonBuilder
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.concurrent.thread

object Playground {

    @JvmStatic
    fun main(args: Array<String>) {
        val path = args[0]
        Files.newBufferedReader(Paths.get(path)).use { reader ->
            val gson = GsonBuilder().create()
            val config = gson.fromJson(reader, Config::class.java)
            if (config != null) {
                loadAndRead(config)
            }
        }
    }

    fun loadAndRead(config: Config) {
        try {
            val schema = Schema(config)

            val id = schema.entityForName("feature_surf")?.columnForName("id", StringColumnType())
            val feature = schema.entityForName("feature_surf")?.columnForName("feature", FloatArrayColumnType())

            val block = {
                val start = System.currentTimeMillis()
                val txf = feature?.getTransaction(AccessorMode.READONLY)
                val txid = id?.getTransaction(AccessorMode.READONLY)
                val first = txf?.read(1L)
                val container = KnnContainer(10000, first!!, Distance.L2)
                txf.scan().forEach { e -> container.add(e.key, e.value) }
                println(String.format("%s: kNN for n=%d, s=%d vectors took %d ms", Thread.currentThread().name, container.knn.size, txf.count(), System.currentTimeMillis() - start))
                val results = container.knn.map { p ->
                    Pair(txid?.read(p.first), p.second);
                }
                println(String.format("%s: kNN + fetching values for n=%d, s=%d vectors took %d ms", Thread.currentThread().name, container.knn.size, txf.count(), System.currentTimeMillis() - start))
            }



            for (i in 1..8) {
                thread(start = true, block =  block);
            }


        } catch (e: IOException) {
            e.printStackTrace()
        }

    }


    @Throws(IOException::class, DatabaseException::class)
    fun loadAndPersist(config: Config) {
        val schema = Schema(config)
        val entity = schema.createEntity("feature_surf")
        entity.createColumn("id", StringColumnType())
        entity.createColumn("feature", FloatArrayColumnType())

        for (i in 1..63) {
            Files.newBufferedReader(Paths.get(String.format("/Users/rgasser/Downloads/Ikarus/Import/features_surfmf25k512_%d.json", i))).use { reader ->
                val gson = GsonBuilder().create()
                val features = gson.fromJson(reader, Array<Feature>::class.java)
                val tx = entity.newTransaction(listOf("id", "feature"), AccessorMode.READWRITE_TX)
                val start = System.currentTimeMillis()
                for (f in features) {
                    tx.insert(mapOf(Pair("id", f.id), Pair("feature", f.feature)));
                }
                tx.commit()
                println(String.format("Writing %d vectors took %d ms", features.size, System.currentTimeMillis() - start))
            }
        }
    }

}