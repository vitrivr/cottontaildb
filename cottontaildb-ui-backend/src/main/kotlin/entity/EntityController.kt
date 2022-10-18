package entity

import ClientConfig
import com.google.gson.Gson
import io.javalin.http.Context
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.nio.file.Path


object EntityController {

    private val gson = Gson()

    //from CreateEntityCommand.kt
    data class ColumnInfo(val name: String, val type: CottontailGrpc.Type, val nullable: Boolean = false, val size: Int = -1) {
        fun toDefinition() : CottontailGrpc.ColumnDefinition {
            val def = CottontailGrpc.ColumnDefinition.newBuilder()
            def.nameBuilder.name = name
            def.type = type
            def.nullable = nullable
            def.length = size
            return def.build()
        }
    }

    fun aboutEntity(context: Context){
        val result = ClientConfig.client.about(AboutEntity(context.pathParam("name")))
        context.json(result)
    }
    fun clearEntity(context: Context){
        val result = ClientConfig.client.delete(Delete(context.pathParam("name")))
        context.json(result)
    }

    fun createEntity(context: Context) {

        val columnDefinition = context.body()
        val columnArray: Array<ColumnInfo> = gson.fromJson(columnDefinition, Array<ColumnInfo>::class.java)
        val list = columnArray.map { it.toDefinition() }

        val temp = context.pathParam("name").split(".")

        val colDef = CottontailGrpc.EntityDefinition.newBuilder()
            .setEntity(CottontailGrpc.EntityName.newBuilder().setName(temp[1])
                .setSchema(CottontailGrpc.SchemaName.newBuilder()
                    .setName(temp[0]).build())
                .build())


        if (list.isNotEmpty()) {
            list.forEach {
                colDef.addColumns(it)
            }
            val result : TupleIterator = ClientConfig.client.create(CottontailGrpc.CreateEntityMessage.newBuilder().setDefinition(colDef).build())
            context.json(result)

        } else {
            TODO()
        }


    }

    fun createIndex(context: Context){
        TODO()
    }
    fun deleteRow(context: Context){
        TODO()
    }
    fun dropEntity(context: Context){
        val result = ClientConfig.client.drop(DropEntity(context.pathParam("name")))
        context.json(result)
    }
    fun dropIndex(context: Context){
        TODO()
    }
    fun dumpEntity(context: Context){


        val format = context.pathParam("format")
        val entityName = context.pathParam("name")

        val path = Path.of(context.pathParam("path"))
            .resolve("${entityName}.${format}")

        val qm = Query(context.pathParam("name"))

        TODO()


    }
    fun importData(context: Context){
        TODO()
    }
    fun listAllEntities(context: Context){
        TODO()
    }
    fun optimizeEntity(context: Context){
        val result = ClientConfig.client.optimize(OptimizeEntity(context.pathParam("name")))
        context.json(result)
    }
    fun truncateEntity(context: Context){
        val result = ClientConfig.client.truncate(TruncateEntity(context.pathParam("name")))
        context.json(result)
    }

}