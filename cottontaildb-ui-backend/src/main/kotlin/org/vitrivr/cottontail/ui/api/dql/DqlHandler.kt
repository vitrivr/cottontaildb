package org.vitrivr.cottontail.ui.api.dql

import io.javalin.http.Context
import io.javalin.openapi.*
import org.vitrivr.cottontail.ui.api.database.obtainClientForContext
import org.vitrivr.cottontail.ui.model.dbo.Entity
import org.vitrivr.cottontail.ui.model.status.ErrorStatus
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */

@OpenApi(
    path = "/api/{connection}/{schema}/{entity}/preview",
    methods = [HttpMethod.GET],
    summary = "Previews data for the entity specified by the connection string The preview can be customised through parameters.",
    operationId = "getEntityPreview",
    tags = ["DQL", "Entity"],
    queryParams = [
        OpenApiParam(name = "limit", description = "The maximum size of the result set. Used for pagination.", required = true, type = Int::class),
        OpenApiParam(name = "skip", description = "The number of items to skip in the result set. Used for pagination.", required = true, type = Int::class),
        OpenApiParam(name = "sort", description = "The name of hte column to sort by.", required = false),
    ],
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true),
        OpenApiParam(name = "schema", description = "Name of the schema the entity belongs to.", required = true),
        OpenApiParam(name = "entity", description = "Name of the entity to list details about.", required = true)
    ],
    responses = [
        OpenApiResponse("200", [OpenApiContent(Entity::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("404", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun preview(context: Context) {
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    val entityName = context.pathParam("entity")
    val limit = context.queryParam("limit") ?: throw ErrorStatusException(400, "The 'limit' query parameter must be set.")
    val skip = context.queryParam("skip") ?: throw ErrorStatusException(400, "The 'skip' query parameter must be set.")

}