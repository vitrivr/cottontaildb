package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.model.basics.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException

import com.google.gson.GsonBuilder
import kotlinx.serialization.json.JSON

import java.nio.file.Files
import java.nio.file.Paths


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

        val catalogue = Catalogue(config)
        val schema = catalogue.getSchema("cottontail")
        val entity = schema.getEntity("test")

        val engine = ch.unibas.dmi.dbis.cottontail.execution.ExecutionEngine(config)

        val plan = engine.newExecutionPlan()
        val d = 512
        val n = 10000


        //val kNNTask = FullscanFloatKnnTask(entity, ColumnDef.withAttributes("feature","FLOAT_VEC", d), VectorUtility.randomFloatVector(d), Distance.L2, n)

        //plan.addTask(kNNTask)
        //plan.addTask(ColumnProjectionTask(entity, ColumnDef.withAttributes("id","STRING")), kNNTask.id)


        val results = plan.execute()
        println("Success!")
    }


    fun loadAndPersist(config: Config) {
        val catalogue = Catalogue(config)
        catalogue.createSchema("cottontail")

        /* Load schema. */
        val schema = catalogue.getSchema("cottontail")
        schema.createEntity("test",  ColumnDef.withAttributes("id","STRING"),  ColumnDef.withAttributes("feature","FLOAT_VEC", 2048))
        val entity = schema.getEntity("test")

        for (i in 1..76) {
            val path = Paths.get("/Users/gassra02/Downloads/data/features_conceptmasksade20k_$i.json")
            if (Files.exists(path)) {
                Files.newBufferedReader(path).use { reader ->
                    val gson = GsonBuilder().create()
                    val features = gson.fromJson(reader, Array<Feature>::class.java)
                    val start = System.currentTimeMillis()
                    try {
                        entity.Tx(readonly = false).begin {
                            for (f in features) {
                                it.insert(StandaloneRecord(null, arrayOf(ColumnDef.withAttributes("id","STRING"), ColumnDef.withAttributes("feature","FLOAT_VEC", 2048)), arrayOf(f.id, f.feature)))
                            }
                            true
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