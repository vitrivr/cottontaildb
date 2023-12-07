package org.vitrivr.cottontail.ui.api.dml

import io.grpc.Status
import io.grpc.StatusException
import io.javalin.http.BadRequestResponse
import io.javalin.http.Context
import io.javalin.openapi.*
import org.vitrivr.cottontail.client.language.basics.predicate.Predicate
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.ui.api.database.obtainClientForContext
import org.vitrivr.cottontail.ui.model.status.ErrorStatus
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException
import org.vitrivr.cottontail.ui.model.status.SuccessStatus


@OpenApi(
    path = "/api/{connection}/{schema}/{entity}/delete",
    methods = [HttpMethod.DELETE],
    summary = "Deletes an entry specified by the connection string and the provided key and value.",
    operationId = "deleteRecord",
    tags = ["DML", "Delete"],
    requestBody = OpenApiRequestBody([OpenApiContent(Predicate::class)], required = false),
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true),
        OpenApiParam(name = "schema", description = "Name of the schema the entity belongs to.", required = true),
        OpenApiParam(name = "entity", description = "Name of the entity to drop.", required = true)
    ],
    responses = [
        OpenApiResponse("200", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("404", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun deleteFromEntity(context: Context) {
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    val entityName = context.pathParam("entity")
    val predicate = try {
        context.bodyAsClass(Predicate::class.java)
    } catch (e: BadRequestResponse) {
        null
    }

    try {
        val query = Delete("$schemaName.$entityName")
        var deleted = 0L
        if (predicate != null) {
            query.where(predicate)
        }
        client.delete(query).forEach {
            deleted = it.asLong("deleted") ?: 0L
        }
        context.json(SuccessStatus("Successfully deleted $deleted entries from $entityName.$schemaName."))
    } catch (e: StatusException){
        when (e.status) {
            Status.NOT_FOUND -> throw ErrorStatusException(404, "Failed to delete entries from $entityName.$schemaName, because entity does not exist.")
            else -> throw ErrorStatusException(500, "Failed to delete entries from $entityName.$schemaName.")
        }
    }
}