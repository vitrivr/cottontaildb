package org.vitrivr.cottontail.ui.api.dql

import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.javalin.http.Context
import io.javalin.openapi.*
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.values.PublicValue
import org.vitrivr.cottontail.ui.api.database.drainToList
import org.vitrivr.cottontail.ui.api.database.obtainClientForContext
import org.vitrivr.cottontail.ui.model.results.Column
import org.vitrivr.cottontail.ui.model.results.Resultset
import org.vitrivr.cottontail.ui.model.status.ErrorStatus
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException


@OpenApi(
    path = "/api/{connection}/{schema}/{entity}/preview",
    methods = [HttpMethod.GET],
    summary = "Previews data for the entity specified by the connection string The preview can be customised through parameters.",
    operationId = "getEntityPreview",
    tags = ["DQL", "Entity"],
    queryParams = [
        OpenApiParam(name = "limit", description = "The maximum size of the result set. Used for pagination.", required = true, type = Long::class),
        OpenApiParam(name = "skip", description = "The number of items to skip in the result set. Used for pagination.", required = true, type = Long::class),
        OpenApiParam(name = "sortColumn", description = "The name of the column to sort by.", required = false),
        OpenApiParam(name = "sortDirection", description = "The requested sort direct.", required = false),
    ],
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true),
        OpenApiParam(name = "schema", description = "Name of the schema the entity belongs to.", required = true),
        OpenApiParam(name = "entity", description = "Name of the entity to list details about.", required = true)
    ],
    responses = [
        OpenApiResponse("200", [OpenApiContent(Resultset::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("404", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun previewEntity(context: Context) {
    val client = context.obtainClientForContext()
    val schemaName = context.pathParam("schema")
    val entityName = context.pathParam("entity")
    val limit = context.queryParam("limit")?.toLongOrNull() ?: throw ErrorStatusException(400, "The 'limit' query parameter must be set.")
    val skip = context.queryParam("skip")?.toLongOrNull() ?: throw ErrorStatusException(400, "The 'skip' query parameter must be set.")
    val sortColumn = context.queryParam("sortColumn")
    val sortDirection = try {
        Direction.valueOf(context.queryParam("sortDirection")?.uppercase() ?: "ASC")
    } catch (e: IllegalArgumentException) {
        throw ErrorStatusException(400, "The sort direction was not recognized.")
    }

    var query = Query("${schemaName}.${entityName}").limit(limit).skip(skip)
    if (sortColumn != null) {
      query = query.order(sortColumn, sortDirection)
    }

    try {
        /** Iterate through schemas and create list. */
        var count = 0L
        client.query(Query("${schemaName}.${entityName}").count()).forEach {
            count = it.asLong(0)!!
        }
        val iterator: TupleIterator = client.query(query)
        val results = iterator.drainToList { t -> t.values().filterIsInstance<PublicValue?>().toTypedArray() }
        context.json(Resultset(iterator.columns.map { Column(it.name.schema, it.type) }, results, count))
    } catch (e: StatusRuntimeException) {
        when (e.status.code) {
            Status.Code.NOT_FOUND -> throw ErrorStatusException(404, "The requested entity '${schemaName}.${entityName} could not be found.")
            Status.Code.UNAVAILABLE -> throw ErrorStatusException(503, "Connection is currently not available.")
            else -> throw ErrorStatusException(500, "Failed to list schemas for connection: ${e.message}")
        }
    }
}