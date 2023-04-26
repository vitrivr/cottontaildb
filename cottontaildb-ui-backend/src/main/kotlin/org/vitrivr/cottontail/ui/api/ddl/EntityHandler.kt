package org.vitrivr.cottontail.ui.api.ddl

import com.google.gson.Gson
import io.grpc.Status
import io.grpc.StatusException
import io.javalin.http.BadRequestResponse
import io.javalin.http.Context
import io.javalin.openapi.*
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.ui.api.database.drainToList
import org.vitrivr.cottontail.ui.api.database.obtainClientForContext
import org.vitrivr.cottontail.ui.model.dbo.Column
import org.vitrivr.cottontail.ui.model.dbo.Entity
import org.vitrivr.cottontail.ui.model.dbo.Index
import org.vitrivr.cottontail.ui.model.status.ErrorStatus
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException
import org.vitrivr.cottontail.ui.model.status.SuccessStatus
import java.util.LinkedList

@Suppress("unused")
class DeleteDetails(details: Tuple){
    var deleted: Long? = details.asLong("deleted")
    var duration_ms : Double? = details.asDouble("duration_ms")
}

class ColumnEntry (val column: String, val value: Any?)

data class IndexInfo(val index : CottontailGrpc.IndexType, val skipBuild : Boolean)


@OpenApi(
    summary = "Lists all entities in the database and schema specified by the connection string.",
    path = "/api/{connection}/{schema}/list",
    tags = ["DDL", "Entity"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.GET],
    responses = [
        OpenApiResponse("200", [OpenApiContent(Array<String>::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("404", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun listEntities(context: Context){
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")

    /* Prepare query and empty list all entities. */
    try {
        val result = client.list(ListEntities(schemaName)).drainToList {
            it.asString(0)!! /* This cannot be null. */
        }
        context.json(result)
    } catch (e: StatusException) {
        when (e.status) {
            Status.NOT_FOUND -> throw ErrorStatusException(404, "Failed to list entities because schema $schemaName does not exist.")
            else -> throw ErrorStatusException(500, "Failed to list entities for schema $schemaName.")
        }
    }
}

@OpenApi(
    summary = "Lists details about the entity specified by the connection string.",
    path = "/api/{connection}/{schema}/{entity}",
    tags = ["DDL", "Entity"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.GET],
    responses = [
        OpenApiResponse("200", [OpenApiContent(Entity::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("404", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun aboutEntity(context: Context) {
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    val entityName = context.pathParam("entity")

    try {
        val result = client.about(AboutEntity(context.pathParam("name")))
        val columns = LinkedList<Column>()
        val indexes = LinkedList<Index>()
        result.forEach {
            if (it.asString("class") == "COLUMN") {
                columns.add(Column(it.asString("dbo")!!, Type.valueOf(it.asString("type")!!), it.asInt("lsize")!!, it.asBoolean("nullable")!!))
            } else if (it.asString("class") == "INDEX") {
                indexes.add(Index(it.asString("dbo")!!, it.asString("type")!!))
            }
        }
        context.json(Entity(entityName, columns, indexes))
    } catch (e: StatusException) {
        when (e.status) {
            Status.NOT_FOUND -> throw ErrorStatusException(404, "Failed to obtain information about entity $entityName.$schemaName, because it does not exist.")
            else -> throw ErrorStatusException(500, "Failed to obtain information about entity $entityName.$schemaName.")
        }
    }
}

@OpenApi(
    summary = "Creates the entity specified by the connection string.",
    path = "/api/{connection}/{schema}/{entity}",
    tags = ["DDL", "Entity"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.POST],
    requestBody = OpenApiRequestBody([OpenApiContent(Array<Column>::class)]),
    responses = [
        OpenApiResponse("200", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("404", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun createEntity(context: Context) {
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    val entityName = context.pathParam("entity")
    val columns = try {
        context.bodyAsClass(Array<Column>::class.java)
    } catch (e: BadRequestResponse) {
        throw ErrorStatusException(400, "Invalid column specifications. This is a programmers error!")
    }

    val request = CreateEntity("$schemaName.$entityName")
    for (c in columns) {
        request.column(c.name, c.type, c.length, c.nullable, c.autoIncrement)
    }
    try {
        client.create(request).close()
        context.json(SuccessStatus("Entity $entityName.$schemaName created successfully."))
    } catch (e: StatusException) {
        when (e.status) {
            Status.ALREADY_EXISTS -> throw ErrorStatusException(404, "Failed to obtain create entity $entityName.$schemaName, because it already existss.")
            Status.NOT_FOUND -> throw ErrorStatusException(404, "Failed to obtain create entity $entityName.$schemaName, because schema does not exist.")
            else -> throw ErrorStatusException(500, "Failed to obtain information about entity $entityName.$schemaName.")
        }
    }
}

@OpenApi(
    summary = "Drops the entity specified by the connection string.",
    path = "/api/{connection}/{schema}/{entity}",
    tags = ["DDL", "Entity"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.DELETE],
    responses = [
        OpenApiResponse("200", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("404", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun dropEntity(context: Context){
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    val entityName = context.pathParam("entity")

    try {
        client.drop(DropEntity("$schemaName.$entityName")).close()
        context.json(SuccessStatus("Entity $entityName.$schemaName dropped successfully."))
    } catch (e: StatusException){
        when (e.status) {
            Status.NOT_FOUND -> throw ErrorStatusException(404, "Failed to drop entity $entityName.$schemaName, because it does not exist.")
            else -> throw ErrorStatusException(500, "Failed to drop entity $entityName.$schemaName.")
        }
    }
}

@OpenApi(
    summary = "Drops the entity specified by the connection string.",
    path = "/api/{connection}/{schema}/{entity}",
    tags = ["DDL", "Entity"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.DELETE],
    responses = [
        OpenApiResponse("200", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("404", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun truncateEntity(context: Context){
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    val entityName = context.pathParam("entity")

    try {
        client.truncate(TruncateEntity("$schemaName.$entityName")).close()
        context.json(SuccessStatus("Entity $entityName.$schemaName truncated successfully."))
    } catch (e: StatusException){
        when (e.status) {
            Status.NOT_FOUND -> throw ErrorStatusException(404, "Failed to truncate entity $entityName.$schemaName, because it does not exist.")
            else -> throw ErrorStatusException(500, "Failed to truncate entity $entityName.$schemaName.")
        }
    }

    val result = client.truncate(TruncateEntity(context.pathParam("name")))
    context.json(result)
}


/*fun createIndex(context: Context){

    val client = initClient(context)
    val indexName = context.pathParam("name")

    val i = indexName.lastIndexOf(".")
    val columnName = indexName.substring(i+1)
    val entityName = indexName.substring(0,i)

    println("$indexName $i $columnName $entityName")

    val indexDefinition: IndexInfo = gson.fromJson(context.body(), IndexInfo::class.java)

    val create = CreateIndex(entityName, columnName, indexDefinition.index)
    if (!indexDefinition.skipBuild) {
        //create.rebuild()
    }
    //client.create(create.rebuild())
}

fun deleteRow(context: Context){

    val client = initClient(context)

    val entity = context.queryParam("org/vitrivr/cottontail/ui/api/entity")
    val column = context.queryParam("column")
    val operator = context.queryParam("operator")
    val value = context.queryParam("value")
    val typedValue : Any
    val type = context.queryParam("type")

    require(value != null) {context.status(400)}

    try {
        require(type != null) {context.status(400)}
        typedValue = convertType(type, value)

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

fun dropIndex(context: Context){
    val client = initClient(context)

    val result = client.drop(DropIndex(context.pathParam("name")))
    context.json(result)
}
fun dumpEntity(context: Context){
    //TODO
}

fun importData(context: Context){
    //TODO
}

fun insertRow(context: Context){
    try {
        val client = initClient(context)
        val entity = context.pathParam("name")
        val insertions = gson.fromJson(context.body(), Array<ColumnEntry>::class.java)

        require(entity != "")
        var insert = Insert().into(entity)

        for (item in insertions) {
            insert = insert.value(item.column, item.value)
        }
        client.insert(insert)
        context.status(201)
    } catch (e: Error) {
        context.status(400)
    }
}
fun listAllEntities(context: Context){
    //TODO
}
fun optimizeEntity(context: Context){
    val client = initClient(context)

    //val result = client.optimize(OptimizeEntity(context.pathParam("name")))
    //context.json(result)
}


fun update(context: Context){

    val client = initClient(context)
    val entity = context.queryParam("org/vitrivr/cottontail/ui/api/entity")
    val column = context.queryParam("column")
    val operator = context.queryParam("operator")
    val value = context.queryParam("value")
    val typedValue : Any
    val type = context.queryParam("type")

    val updateValues = gson.fromJson(context.body(), Array<ColumnEntry>::class.java)

    require(value != null) { context.status(400) }
    require(type != null) { context.status(400) }

    typedValue = convertType(type, value)

    println(entity + operator + column + value + type)

    require(entity != null && column != null && operator != null) { context.status(400) }


    var update = Update().from(entity).where(Expression(column,operator,typedValue))

    updateValues.forEach {
        update = update.values(Pair(it.column, it.value))
    }

    client.update(update)
    context.status(200)
}

private fun convertType(type: String, value: String) : Any {
    when(type) {
        "SHORT" -> return value.toShort()
        "LONG" -> return value.toLong()
        "INTEGER" -> return value.toInt()
        "DOUBLE" -> return value.toDouble()
        "BOOLEAN" -> return value.toBoolean()
        "BYTE" -> return value.toByte()
        "FLOAT" -> return value.toFloat()
        "DATE" -> return LocalDate.parse(value)
        "FLOAT_VEC" -> return gson.fromJson(value, Array<Float>::class.java)
        "LONG_VEC" -> return gson.fromJson(value, Array<Long>::class.java)
        "INT_VEC" -> return gson.fromJson(value, Array<Int>::class.java)
        "BOOL_VEC" -> return gson.fromJson(value, Array<Boolean>::class.java)
        "COMPLEX_32" -> return gson.fromJson(value, Types.Complex32Vector::class.java)
        "COMPLEX_64" -> return gson.fromJson(value, Types.Complex64Vector::class.java)
        "BYTESTRING" -> return gson.fromJson(value, Types.ByteString::class.java)
        else -> return value
    }
}
}*/