package org.vitrivr.cottontail.ui.api.session

import io.javalin.http.BadRequestResponse
import io.javalin.http.Context
import io.javalin.openapi.*
import org.vitrivr.cottontail.ui.model.session.Connection
import org.vitrivr.cottontail.ui.model.status.ErrorStatus
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException
import org.vitrivr.cottontail.ui.model.status.SuccessStatus

@OpenApi(
    summary = "Creates and connects a new Cottontail DB connection.",
    path = "/api/session/connect",
    tags = ["Session"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.POST],
    requestBody = OpenApiRequestBody([OpenApiContent(Connection::class)]),
    responses = [
        OpenApiResponse("200", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("401", [OpenApiContent(ErrorStatus::class)])
    ]
)
fun connect(ctx: Context): SuccessStatus {
    val session = ctx.session()
    val connection = try {
        ctx.bodyAsClass(Connection::class.java)
    } catch (e: BadRequestResponse) {
        throw ErrorStatusException(400, "Invalid parameters. This is a programmers error!")
    }
    if (session.connect(connection)) {
        return SuccessStatus("Connection to $connection established successfully.")
    } else {
        throw ErrorStatusException(400, "Failed to establish connection because connection already exists.")
    }
}

@OpenApi(
    summary = "Disconnects an existing and connected Cottontail DB connection.",
    path = "/api/session/disconnect",
    tags = ["Session"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.POST],
    requestBody = OpenApiRequestBody([OpenApiContent(Connection::class)]),
    responses = [
        OpenApiResponse("200", [OpenApiContent(SuccessStatus::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("401", [OpenApiContent(ErrorStatus::class)])
    ]
)
fun disconnect(ctx: Context): SuccessStatus {
    val session = ctx.session()
    val connection = try {
        ctx.bodyAsClass(Connection::class.java)
    } catch (e: BadRequestResponse) {
        throw ErrorStatusException(400, "Invalid parameters. This is a programmers error!")
    }
    if (session.disconnect(connection)) {
        return SuccessStatus("Disconnected from $connection successfully.")
    } else {
        throw ErrorStatusException(400, "Failed to disconnect connection because connection does not exist.")
    }
}

@OpenApi(
    summary = "Adds a new media collection.",
    path = "/api/session/connections",
    tags = ["Session"],
    operationId = OpenApiOperation.AUTO_GENERATE,
    methods = [HttpMethod.GET],
    responses = [
        OpenApiResponse("200", [OpenApiContent(Array<Connection>::class)])
    ]
)
fun connections(ctx: Context): List<Connection> {
    val session = ctx.session()
    return session.list()
}

/**
 * Extracts the [Session] from the [Context] or creates a new one if none exists.
 */
fun Context.session(): Session {
    var session = this.sessionAttribute<Session>(Session.SESSION_ATTRIBUTE_KEY)
    if (session == null) {
        session = Session()
        this.sessionAttribute(Session.SESSION_ATTRIBUTE_KEY, session)
    }
    return session
}