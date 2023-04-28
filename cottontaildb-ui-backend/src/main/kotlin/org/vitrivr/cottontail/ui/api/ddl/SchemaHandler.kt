package org.vitrivr.cottontail.ui.api.ddl

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
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
    path = "/api/{connection}/{schema}",
    methods = [HttpMethod.POST],
    summary = "Creates a new schema in the database specified by the connection string.",
    operationId = OpenApiOperation.AUTO_GENERATE,
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
    summary = "Creates a new schema in the database specified by the connection string.",
    operationId = OpenApiOperation.AUTO_GENERATE,
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
    path = "/api/{connection}/list",
    methods = [HttpMethod.GET],
    summary = "Lists all schemas in the database specified by the connection string.",
    operationId = OpenApiOperation.AUTO_GENERATE,
    tags = ["DDL", "Schema"],
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true)
    ],
    responses = [
        OpenApiResponse("200", [OpenApiContent(Array<Schema>::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("503", [OpenApiContent(ErrorStatus::class)])
    ]
)
fun listSchemas(context: Context) {
    val client = context.obtainClientForContext()

    /** using ClientConfig's client, sending ListSchemas message to Cottontail DB. */
    try {
        val result: TupleIterator = client.list(ListSchemas())
        val schemas: MutableList<Schema> = mutableListOf()

        /** Iterate through schemas and create list. */
        result.forEach {
            val string = it.asString(0)
            if (!string.isNullOrBlank()) {
                schemas.add(Schema(string!!.split('.')[1], string))
            }
        }
        context.json(schemas)
    } catch (e: StatusRuntimeException) {
        when (e.status.code) {
            Status.Code.UNAVAILABLE -> throw ErrorStatusException(503, "Connection is currently not available.")
            else -> throw ErrorStatusException(500, "Failed to list schemas for connection: ${e.message}")
        }
    }
}