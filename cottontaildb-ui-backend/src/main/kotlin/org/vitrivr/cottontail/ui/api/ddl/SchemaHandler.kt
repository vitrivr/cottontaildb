package org.vitrivr.cottontail.ui.api.ddl

import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.javalin.http.Context
import io.javalin.openapi.*
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.client.language.ddl.ListSchemas
import org.vitrivr.cottontail.ui.api.database.drainToArray
import org.vitrivr.cottontail.ui.api.database.obtainClientForContext
import org.vitrivr.cottontail.ui.model.dbo.Dbo
import org.vitrivr.cottontail.ui.model.status.ErrorStatus
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException
import org.vitrivr.cottontail.ui.model.status.SuccessStatus

@OpenApi(
    path = "/api/{connection}/{schema}",
    methods = [HttpMethod.POST],
    summary = "Creates a new schema in the database specified by the connection string.",
    operationId = "postCreateSchema",
    tags = ["DDL", "Schema"],
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true),
        OpenApiParam(name = "schema", description = "Name of the schema to create.", required = true)
    ],
    responses = [
        OpenApiResponse("201", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("503", [OpenApiContent(ErrorStatus::class)])
    ]
)
fun createSchema(context: Context){
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    try {
        client.create(CreateSchema(schemaName)).close()
        context.status(201).json(SuccessStatus("Schema $schemaName created successfully."))
    } catch (e: StatusRuntimeException) {
        when (e.status.code) {
            Status.Code.ALREADY_EXISTS -> throw ErrorStatusException(400, "Schema $schemaName already exists.")
            Status.Code.UNAVAILABLE -> throw ErrorStatusException(503, "Connection is currently not available.")
            else -> throw ErrorStatusException(500, "Failed to create schema $schemaName: ${e.message}")
        }
    }
}

@OpenApi(
    path = "/api/{connection}/{schema}",
    methods = [HttpMethod.DELETE],
    summary = "Drops an existing schema in the database specified by the connection string.",
    operationId = "deleteDropSchema",
    tags = ["DDL", "Schema"],
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true),
        OpenApiParam(name = "schema", description = "Name of the schema to drop.", required = true)
    ],
    responses = [
        OpenApiResponse("200", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("503", [OpenApiContent(ErrorStatus::class)])
    ]
)
fun dropSchema(context: Context){
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    try {
        client.drop(DropSchema(schemaName)).close()
        context.status(200).json(SuccessStatus("Schema $schemaName dropped successfully."))
    } catch (e: StatusRuntimeException) {
        when (e.status.code) {
            Status.Code.NOT_FOUND -> throw ErrorStatusException(400, "Schema $schemaName does not exist.")
            Status.Code.UNAVAILABLE -> throw ErrorStatusException(503, "Connection is currently not available.")
            else -> throw ErrorStatusException(500, "Failed to drop schema $schemaName: ${e.message}")
        }
    }
}

@OpenApi(
    path = "/api/{connection}",
    methods = [HttpMethod.GET],
    summary = "Lists all schemas in the database specified by the connection string.",
    operationId = "getListSchema",
    tags = ["DDL", "Schema"],
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true)
    ],
    responses = [
        OpenApiResponse("200", [OpenApiContent(Array<Dbo>::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("503", [OpenApiContent(ErrorStatus::class)])
    ]
)
fun listSchemas(context: Context) {
    val client = context.obtainClientForContext()

    /** using ClientConfig's client, sending ListSchemas message to Cottontail DB. */
    try {
        /** Iterate through schemas and create list. */
        val schemas = client.list(ListSchemas()).drainToArray {
            Dbo.schema(it.asString(0)!!)
        }
        context.json(schemas)
    } catch (e: StatusRuntimeException) {
        when (e.status.code) {
            Status.Code.UNAVAILABLE -> throw ErrorStatusException(503, "Connection is currently not available.")
            else -> throw ErrorStatusException(500, "Failed to list schemas for connection: ${e.message}")
        }
    }
}