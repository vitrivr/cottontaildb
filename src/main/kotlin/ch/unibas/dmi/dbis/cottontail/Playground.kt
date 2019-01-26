package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.column.FloatArrayColumnType
import ch.unibas.dmi.dbis.cottontail.database.column.StringColumnType
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlan
import ch.unibas.dmi.dbis.cottontail.execution.tasks.knn.FullscanFloatKnnTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.projection.ColumnProjectionTask
import ch.unibas.dmi.dbis.cottontail.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset
import ch.unibas.dmi.dbis.cottontail.model.basics.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.utilities.VectorUtility

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
                loadAndRead(config)
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

        val init = plan.addTask(FullscanFloatKnnTask(entity, ColumnDef.withAttributes("feature","FLOAT_VEC", d), VectorUtility.randomFloatVector(d), Distance.L2, n))
        plan.addOutput(ColumnProjectionTask(entity, ColumnDef.withAttributes("id","STRING")))

        val start = System.currentTimeMillis()
        plan.execute()
       // println("kNN for n=$d, s=${result.first.result.columns} vectors took ${System.currentTimeMillis() - start} ms.")
    }


    fun loadAndPersist(config: Config) {


        Catalogue.init(config)

        val catalogue = Catalogue(config)
        catalogue.createSchema("cottontail")

        /* Load schema. */
        val schema = catalogue.getSchema("cottontail")
        schema.createEntity("test", ColumnDef("id", StringColumnType()), ColumnDef("feature", FloatArrayColumnType(), 512))
        val entity = schema.getEntity("test")

        for (i in 1..76) {
            Files.newBufferedReader(Paths.get("/Volumes/Data (Mac)/Extracted/features_surfmf25k512_$i.json")).use { reader ->
                val gson = GsonBuilder().create()
                val features = gson.fromJson(reader, Array<Feature>::class.java)
                val start = System.currentTimeMillis()
                entity.Tx(readonly = false).begin {
                    for (f in features) {
                        it.insert(StandaloneRecord(null, ColumnDef.withAttributes("id","STRING"), ColumnDef.withAttributes("feature","FLOAT_VEC")).assign(f.id, f.feature))
                    }
                    true
                }
                println("Writing ${features.size} vectors took ${System.currentTimeMillis() - start} ms.")
            }
        }
    }
}