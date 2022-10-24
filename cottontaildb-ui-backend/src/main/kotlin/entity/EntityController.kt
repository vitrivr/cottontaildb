package entity

import ClientConfig
import com.google.gson.Gson
import io.javalin.http.Context
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexType
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

    data class IndexInfo(val skipBuild : Boolean, val attribute : String, val index : IndexType){}


    fun aboutEntity(context: Context) {

        val result = ClientConfig.client.about(AboutEntity(context.pathParam("name")))
        val entityDetails: MutableList<EntityDetails> = mutableListOf()

        result.forEach {
            entityDetails.add(EntityDetails(it))
        }
        println(entityDetails)
        context.json(entityDetails)

    }

    fun clearEntity(context: Context){
        val result = ClientConfig.client.delete(Delete(context.pathParam("name")))
        context.json(result)
    }

    fun createEntity(context: Context) {

        val columnArray: Array<ColumnInfo> = gson.fromJson(context.body(), Array<ColumnInfo>::class.java)
        val list = columnArray.map { it.toDefinition() }

        val temp = context.pathParam("name").split(".")

        val columnDefinition = CottontailGrpc.EntityDefinition.newBuilder()
            .setEntity(CottontailGrpc.EntityName.newBuilder().setName(temp[1])
                .setSchema(CottontailGrpc.SchemaName.newBuilder()
                    .setName(temp[0]).build())
                .build())


        if (list.isNotEmpty()) {
            list.forEach {
                columnDefinition.addColumns(it)
            }
            val result : TupleIterator = ClientConfig.client.create(CottontailGrpc.CreateEntityMessage.newBuilder().setDefinition(columnDefinition).build())
            context.json(result)

        } else {
            TODO()
        }


    }

    fun createIndex(context: Context){

        val indexDefinition: IndexInfo = gson.fromJson(context.body(), IndexInfo::class.java)

        val create = CreateIndex(context.pathParam("name"), indexDefinition.attribute, indexDefinition.index)
        if (!indexDefinition.skipBuild) {
            create.rebuild()
        }

        ClientConfig.client.create(create.rebuild())
        }
    fun deleteRow(context: Context){
        TODO()
    }
    fun dropEntity(context: Context){
        val result = ClientConfig.client.drop(DropEntity(context.pathParam("name")))
        context.json(result)
    }
    fun dropIndex(context: Context){
        println("recieved")
        val result = ClientConfig.client.drop(DropIndex(context.pathParam("name")))
        context.json(result)
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