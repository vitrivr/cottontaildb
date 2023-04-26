package org.vitrivr.cottontail.ui.api.ddl

import io.grpc.Status
import io.grpc.StatusException
import io.javalin.http.Context
import io.javalin.openapi.*
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.client.language.ddl.ListSchemas
import org.vitrivr.cottontail.ui.api.database.obtainClientForContext
import org.vitrivr.cottontail.ui.model.dbo.Schema
import org.vitrivr.cottontail.ui.model.status.ErrorStatus
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException
import org.vitrivr.cottontail.ui.model.status.SuccessStatus

@OpenApi(
    summary = "Creates a new schema in the database specified by the connection string.",
    path = "/api/{connection}/{schema}",
    tags = ["DDL", "Schema"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.POST],
    responses = [
        OpenApiResponse("201", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun createSchema(context: Context){
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    try {
        client.create(CreateSchema(schemaName)).close()
        context.status(201).json(SuccessStatus("Schema $schemaName created successfully."))
    } catch (e: StatusException) {
        when (e.status) {
            Status.ALREADY_EXISTS -> throw ErrorStatusException(400, "Schema $schemaName already exists.")
            else -> throw ErrorStatusException(400, "Failed to create schema $schemaName.")
        }
    }
}

@OpenApi(
    summary = "Creates a new schema in the database specified by the connection string.",
    path = "/api/{connection}/{schema}",
    tags = ["DDL", "Schema"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.DELETE],
    responses = [
        OpenApiResponse("200", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun dropSchema(context: Context){
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    try {
        client.drop(DropSchema(schemaName)).close()
        context.status(200).json(SuccessStatus("Schema $schemaName dropped successfully."))
    } catch (e: StatusException) {
        when (e.status) {
            Status.NOT_FOUND -> throw ErrorStatusException(400, "Schema $schemaName does not exist.")
            else -> throw ErrorStatusException(400, "Failed to drop schema $schemaName.")
        }
    }
}

@OpenApi(
    summary = "Lists all schemas in the database specified by the connection string.",
    path = "/api/{connection}/list",
    tags = ["DDL", "Schema"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.GET],
    responses = [
        OpenApiResponse("200", [OpenApiContent(Array<Schema>::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun listSchemas(context: Context) {
    val client = context.obtainClientForContext()

    /** using ClientConfig's client, sending ListSchemas message to Cottontail DB. */
    val result: TupleIterator = client.list(ListSchemas())
    val schemas: MutableList<Schema> = mutableListOf()

    /** Iterate through schemas and create list. */
    result.forEach {
        if (!it.asString(0).isNullOrBlank()){ /** first value of tuple is the name */
            schemas.add(Schema(it[0].toString()))
        }
    }
    context.json(schemas)
}