package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.knn.KnnContainer
import ch.unibas.dmi.dbis.cottontail.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.model.DatabaseException
import com.google.gson.GsonBuilder
import org.db.mapdb.list.LinkedListFactory
import org.mapdb.*
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
            val path = Paths.get(config.dataFolder).resolve("pdlinkedlist.db");
            val list = LinkedListFactory(path, PairSerializer)
                    .fileMmapEnable()
                    .readOnly()
                    .make()

            val block = {
                val start = System.currentTimeMillis()
                val container = KnnContainer(10000, list.get(0).second, Distance.L2)

                for (e in list) {
                    container.add(e.first, e.second)
                }
                println(String.format("%s: kNN for n=%d, s=%d vectors took %d ms", Thread.currentThread().name, container.knn.size, list.size, System.currentTimeMillis() - start))
            }


            for (i in 1..16) {
                thread(start = true, block = block);
            }


        } catch (e: IOException) {
            e.printStackTrace()
        }

    }


    @Throws(IOException::class, DatabaseException::class)
    fun loadAndPersist(config: Config) {
        val path = Paths.get(config.dataFolder).resolve("pdlinkedlist.db");
        val list = LinkedListFactory(path, PairSerializer)
                .fileMmapEnable()
                .make()

        var n = 0L
        for (i in 1..63) {
            Files.newBufferedReader(Paths.get(String.format("/Users/rgasser/Downloads/Ikarus/Import/features_surfmf25k512_%d.json", i))).use { reader ->
                val gson = GsonBuilder().create()
                val features = gson.fromJson(reader, Array<Feature>::class.java)
                val start = System.currentTimeMillis()
                for (f in features) {
                    list.add(Pair(n++, f.feature!!))
                    n++
                }
                println(String.format("Writing %d vectors took %d ms", features.size, System.currentTimeMillis() - start))
            }
        }

        list.close()
    }
}


object PairSerializer : Serializer<Pair<Long,FloatArray>> {

    val _inner = Serializer.FLOAT_ARRAY

    override fun serialize(out: DataOutput2, value: Pair<Long, FloatArray>) {
        out.packLong(value.first)
        _inner.serialize(out, value.second)
    }

    override fun deserialize(input: DataInput2, available: Int): Pair<Long, FloatArray> = Pair(input.unpackLong(), _inner.deserialize(input, available))
}