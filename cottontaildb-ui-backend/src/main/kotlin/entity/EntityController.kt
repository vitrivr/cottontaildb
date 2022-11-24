package entity

import channelCache
import com.google.gson.Gson
import initClient
import io.javalin.http.Context
import kotlinx.serialization.json.*
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.basics.predicate.Expression
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.language.dml.Update
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexType
import java.lang.Exception
import java.time.LocalDate


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

    @Suppress("unused")
    class EntityDetails (details: Tuple){
        var dbo: String? = details.asString("dbo")
        var _class : String? = details.asString("class")
        var type : String? = details.asString("type")
        var rows: Int? = details.asInt("rows")
        var lsize : Int? = details.asInt("l_size")
        var nullable : Boolean? = details.asBoolean("nullable")
        val info : String? = details.asString("info")
    }
    @Suppress("unused")
    class DeleteDetails(details: Tuple){
        var deleted: Long? = details.asLong("deleted")
        var duration_ms : Double? = details.asDouble("duration_ms")
    }




    data class IndexInfo(val index : IndexType, val skipBuild : Boolean){}


    fun aboutEntity(context: Context) {

        val client = initClient(context)

        val result = client.about(AboutEntity(context.pathParam("name")))
        val entityDetails: MutableList<EntityDetails> = mutableListOf()

        result.forEach {
            entityDetails.add(EntityDetails(it))
        }
        context.json(entityDetails)

    }

    fun clearEntity(context: Context){
        val client = initClient(context)

        val result = client.delete(Delete(context.pathParam("name")))
        context.json(result)
    }

    fun createEntity(context: Context) {

        val client = initClient(context)

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
            val result : TupleIterator = client.create(CottontailGrpc.CreateEntityMessage.newBuilder().setDefinition(columnDefinition).build())
            context.json(result)

        } else {
            TODO()
        }


    }

    fun createIndex(context: Context){
        val client = initClient(context)

        Query()
        val indexName = context.pathParam("name")

        val i = indexName.lastIndexOf(".")
        val columnName = indexName.substring(i+1)
        val entityName = indexName.substring(0,i)

        val indexDefinition: IndexInfo = gson.fromJson(context.body(), IndexInfo::class.java)

        val create = CreateIndex(entityName, columnName, indexDefinition.index)
        if (!indexDefinition.skipBuild) {
            create.rebuild()
        }
        client.create(create.rebuild())
        }

    fun deleteRow(context: Context){

        val port = context.pathParam("port").toInt()
        val channel = channelCache.get(Pair(port,"localhost"))
        val client = SimpleClient(channel)

        val entity = context.queryParam("entity")
        val column = context.queryParam("column")
        val operator = context.queryParam("operator")
        val value = context.queryParam("value")
        val typedValue : Any
        val type = context.queryParam("type")

        require(value != null)

        try {
            when(type) {
                "SHORT" -> typedValue = value.toShort()
                "LONG" -> typedValue = value.toLong()
                "INTEGER" -> typedValue = value.toInt()
                "DOUBLE" ->  typedValue = value.toDouble()
                "BOOLEAN" -> typedValue = value.toBoolean()
                "BYTE" -> typedValue = value.toByte()
                "FLOAT" -> typedValue = value.toFloat()
                "DATE" -> typedValue = LocalDate.parse(value)
                "FLOAT_VEC" -> typedValue = gson.fromJson(value, Array<Float>::class.java)
                "LONG_VEC" -> typedValue = gson.fromJson(value, Array<Long>::class.java)
                "INT_VEC" -> typedValue = gson.fromJson(value, Array<Int>::class.java)
                "BOOL_VEC" -> typedValue = gson.fromJson(value, Array<Boolean>::class.java)
                "COMPLEX_32" -> typedValue = gson.fromJson(value, Types.Complex32Vector::class.java)
                "COMPLEX_64" -> typedValue = gson.fromJson(value, Types.Complex64Vector::class.java)
                "BYTESTRING" -> typedValue = gson.fromJson(value, Types.ByteString::class.java)
                else -> typedValue = value
            }
            if(entity != null && column != null && operator != null) {
                val result = client.delete(Delete().from(entity).where(Expression(column, operator, typedValue)))
                val deleteDetails: MutableList<DeleteDetails> = mutableListOf()
                result.forEach {
                    deleteDetails.add(DeleteDetails(it))
                }
                context.json(deleteDetails)
            } else {
                context.status(400)
            }
        } catch (e: Exception){
            context.status(400)
        }
    }
    fun dropEntity(context: Context){
        val client = initClient(context)

        val result = client.drop(DropEntity(context.pathParam("name")))
        context.json(result)
    }
    fun dropIndex(context: Context){
        val client = initClient(context)

        val result = client.drop(DropIndex(context.pathParam("name")))
        context.json(result)
    }
    fun dumpEntity(context: Context){
        val client = initClient(context)

        val entityName = context.pathParam("name")
        val qm = Query(entityName)
        val results = client.query(qm)
        val columnNames = results.columnNames
        TODO()
    }

    fun importData(context: Context){
        TODO()
    }

    fun insertRow(context: Context){
        val client = initClient(context)

        val column = context.queryParam("column")
        val value = context.queryParam("value")
        val entity = context.queryParam("entity")

        if(column != null && value != null && entity != null){
            client.insert(Insert().into(entity).value(column,value))
        }
    }
    fun listAllEntities(context: Context){
        TODO()
    }
    fun optimizeEntity(context: Context){
        val client = initClient(context)

        val result = client.optimize(OptimizeEntity(context.pathParam("name")))
        context.json(result)
    }
    fun truncateEntity(context: Context){
        val client = initClient(context)

        val result = client.truncate(TruncateEntity(context.pathParam("name")))
        context.json(result)
    }

    fun update(context: Context){
        val client = initClient(context)

        client.update(Update().from("")
            .where(Expression("","",""))
            .values(Pair("","")))
    }
}