package org.vitrivr.cottontail.ui.api.dml

import io.grpc.Status
import io.grpc.StatusException
import io.javalin.http.Context
import io.javalin.openapi.*
import org.vitrivr.cottontail.client.language.basics.predicate.Expression
import org.vitrivr.cottontail.client.language.ddl.DropEntity
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.ui.api.database.obtainClientForContext
import org.vitrivr.cottontail.ui.model.status.ErrorStatus
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException
import org.vitrivr.cottontail.ui.model.status.SuccessStatus


@OpenApi(
    path = "/api/{connection}/{schema}/{entity}/delete/{column}/{value}",
    methods = [HttpMethod.DELETE],
    summary = "Deletes an entry specified by the connection string and the provided key and value.",
    operationId = OpenApiOperation.AUTO_GENERATE,
    tags = ["DML", "Delete"],
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true),
        OpenApiParam(name = "schema", description = "Name of the schema the entity belongs to.", required = true),
        OpenApiParam(name = "entity", description = "Name of the entity to drop.", required = true),
        OpenApiParam(name = "column", description = "Name of the column to determine the entry to delete.", required = true),
        OpenApiParam(name = "value", description = "Value of the column to determine which entry to delete.", required = true)
    ],
    responses = [
        OpenApiResponse("200", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("404", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun simpleDelete(context: Context) {
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    val entityName = context.pathParam("entity")
    val columnName = context.pathParam("column")
    val value = context.pathParam("value")

    try {
        client.delete(Delete("$schemaName.$entityName").where(Expression(columnName, "=", value))).close()
        context.json(SuccessStatus("Entity $entityName.$schemaName dropped successfully."))
    } catch (e: StatusException){
        when (e.status) {
            Status.NOT_FOUND -> throw ErrorStatusException(404, "Failed to drop entity $entityName.$schemaName, because it does not exist.")
            else -> throw ErrorStatusException(500, "Failed to drop entity $entityName.$schemaName.")
        }
    }
}