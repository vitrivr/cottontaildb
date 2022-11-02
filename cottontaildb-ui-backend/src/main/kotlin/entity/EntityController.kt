package entity

import ClientConfig
import com.google.gson.Gson
import io.javalin.http.Context
import kotlinx.serialization.json.*
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexType


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

    data class IndexInfo(val index : IndexType, val skipBuild : Boolean){}


    fun aboutEntity(context: Context) {

        val clientConfig = ClientConfig(context.pathParam("port").toInt())

        val result = clientConfig.client.about(AboutEntity(context.pathParam("name")))
        val entityDetails: MutableList<EntityDetails> = mutableListOf()

        result.forEach {
            entityDetails.add(EntityDetails(it))
        }
        println(entityDetails)
        context.json(entityDetails)

    }

    fun clearEntity(context: Context){
        val clientConfig = ClientConfig(context.pathParam("port").toInt())

        val result = clientConfig.client.delete(Delete(context.pathParam("name")))
        context.json(result)
    }

    fun createEntity(context: Context) {
        val clientConfig = ClientConfig(context.pathParam("port").toInt())

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
            val result : TupleIterator = clientConfig.client.create(CottontailGrpc.CreateEntityMessage.newBuilder().setDefinition(columnDefinition).build())
            context.json(result)

        } else {
            TODO()
        }


    }

    fun createIndex(context: Context){

        val clientConfig = ClientConfig(context.pathParam("port").toInt())

        val indexName = context.pathParam("name")

        val i = indexName.lastIndexOf(".")
        val columnName = indexName.substring(i+1)
        val entityName = indexName.substring(0,i)

        val indexDefinition: IndexInfo = gson.fromJson(context.body(), IndexInfo::class.java)

        val create = CreateIndex(entityName, columnName, indexDefinition.index)
        if (!indexDefinition.skipBuild) {
            create.rebuild()
        }
        clientConfig.client.create(create.rebuild())
        }

    fun deleteRow(context: Context){
        TODO()
    }
    fun dropEntity(context: Context){
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        val result = clientConfig.client.drop(DropEntity(context.pathParam("name")))
        context.json(result)
    }
    fun dropIndex(context: Context){
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        println("recieved")
        val result = clientConfig.client.drop(DropIndex(context.pathParam("name")))
        context.json(result)
    }
    fun dumpEntity(context: Context){
        val clientConfig = ClientConfig(context.pathParam("port").toInt())

        val entityName = context.pathParam("name")
        val qm = Query(entityName)
        val results = clientConfig.client.query(qm)
        val columnNames = results.columnNames
        TODO()


        }


    fun importData(context: Context){
        TODO()
    }
    fun listAllEntities(context: Context){
        TODO()
    }
    fun optimizeEntity(context: Context){
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        val result = clientConfig.client.optimize(OptimizeEntity(context.pathParam("name")))
        context.json(result)
    }
    fun truncateEntity(context: Context){
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        val result = clientConfig.client.truncate(TruncateEntity(context.pathParam("name")))
        context.json(result)
    }

}