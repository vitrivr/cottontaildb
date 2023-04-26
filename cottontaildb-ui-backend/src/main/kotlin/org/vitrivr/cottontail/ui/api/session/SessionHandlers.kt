package org.vitrivr.cottontail.ui.api.session

import io.javalin.http.BadRequestResponse
import io.javalin.http.Context
import io.javalin.openapi.*
import org.vitrivr.cottontail.ui.model.session.Connection
import org.vitrivr.cottontail.ui.model.session.Session
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
        OpenApiResponse("200", [OpenApiContent(Array<Connection>::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("401", [OpenApiContent(ErrorStatus::class)])
    ]
)
fun connect(ctx: Context) {
    val session = ctx.session()
    val connection = try {
        ctx.bodyAsClass(Connection::class.java)
    } catch (e: BadRequestResponse) {
        throw ErrorStatusException(400, "Invalid parameters. This is a programmers error!")
    }
    if (ConnectionManager.connect(session, connection)) {
        ctx.json(ConnectionManager.list(session))
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
        OpenApiResponse("200", [OpenApiContent(Array<Connection>::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("401", [OpenApiContent(ErrorStatus::class)])
    ]
)
fun disconnect(ctx: Context) {
    val session = ctx.session()
    val connection = try {
        ctx.bodyAsClass(Connection::class.java)
    } catch (e: BadRequestResponse) {
        throw ErrorStatusException(400, "Invalid parameters. This is a programmers error!")
    }
    if (ConnectionManager.disconnect(session, connection)) {
        ctx.json(ConnectionManager.list(session))
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
fun connections(ctx: Context) {
    val session = ctx.session()
    val list = ConnectionManager.list(session)
    ctx.json(list)
}

/**
 * Extracts the [Session] from the [Context] or creates a new one if none exists.
 */
fun Context.session(): Session {
    var session = this.sessionAttribute<Session>(Session.USER_SESSION_KEY)
    if (session == null) {
        session = Session(this.req().session.id, mutableListOf())
        this.sessionAttribute(Session.USER_SESSION_KEY, session)
    }
    return session
}