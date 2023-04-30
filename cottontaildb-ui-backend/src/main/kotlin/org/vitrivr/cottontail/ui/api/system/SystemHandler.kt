package org.vitrivr.cottontail.ui.api.system

import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.javalin.http.Context
import io.javalin.openapi.*
import org.vitrivr.cottontail.ui.api.database.drainToList
import org.vitrivr.cottontail.ui.api.database.obtainClientForContext
import org.vitrivr.cottontail.ui.model.dbo.details.EntityDetails
import org.vitrivr.cottontail.ui.model.status.ErrorStatus
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException
import org.vitrivr.cottontail.ui.model.system.Lock
import org.vitrivr.cottontail.ui.model.system.Transaction
import org.vitrivr.cottontail.ui.model.system.TransactionStatus

@OpenApi(
    path = "/api/{connection}/transactions",
    methods = [HttpMethod.GET],
    summary = "Lists all ongoing transactions in the database specified by the connection string.",
    operationId = "getListTransactions",
    tags = ["System", "Transaction"],
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true)
    ],
    responses = [
        OpenApiResponse("200", [OpenApiContent(Array<Transaction>::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun listTransactions(context: Context) {
    val client = context.obtainClientForContext()
    try {
        val results = client.transactions().drainToList {
            Transaction(
                it.asLong(0)!!,
                it.asString(1)!!,
                TransactionStatus.valueOf(it.asString(2)!!),
                it.asDate(3).toString(),
                it.asDate(4)?.toString(),
                it.asDouble(5)!!
            )
        }

        context.json(results)
    } catch (e: StatusRuntimeException) {
        when (e.status.code) {
            Status.Code.UNAVAILABLE -> throw ErrorStatusException(503, "Connection is currently not available.")
            else -> throw ErrorStatusException(500, "Failed to list transactions for connection: ${e.message}")
        }
    }
}

@OpenApi(
    path = "/api/{connection}/transactions/{txId}",
    methods = [HttpMethod.DELETE],
    summary = "Kills an ongoing transaction in the database specified by the connection string.",
    operationId = "deleteKillTransaction",
    tags = ["System", "Transaction"],
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true),
        OpenApiParam(name = "txId", description = "The numeric transaction ID to kill.", required = true)
    ],
    responses = [
        OpenApiResponse("200", [OpenApiContent(Array<Transaction>::class)]),
        OpenApiResponse("400", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("404", [OpenApiContent(ErrorStatus::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun killTransaction(context: Context) {
    val client = context.obtainClientForContext()
    val txId = context.pathParam("txId")?.toLongOrNull() ?: throw ErrorStatusException(400, "Must specify a valid transaction ID.")
    try {
        client.kill(txId)
    } catch (e: StatusRuntimeException) {
        when (e.status.code) {
            Status.Code.NOT_FOUND -> throw ErrorStatusException(404, "Transaction ${txId} does no longer exist.")
            Status.Code.UNAVAILABLE -> throw ErrorStatusException(503, "Connection is currently not available.")
            else -> throw ErrorStatusException(500, "Failed to kill transaction ${txId}: ${e.message}")
        }
    }
}

@OpenApi(
    path = "/api/{connection}/locks",
    methods = [HttpMethod.GET],
    summary = "Lists details about the entity specified by the connection string.",
    operationId = "getListLocks",
    tags = ["System"],
    pathParams = [
        OpenApiParam(name = "connection", description = "Connection string in the for <host>:<port>.", required = true)
    ],
    responses = [
        OpenApiResponse("200", [OpenApiContent(EntityDetails::class)]),
        OpenApiResponse("500", [OpenApiContent(ErrorStatus::class)]),
    ]
)
fun listLocks(context: Context) {
    val client = context.obtainClientForContext()
    try {
        val result = client.locks().drainToList {
            Lock(it.asString(0)!!, it.asString(1)!!,it.asInt(2)!!, it.asString(3)!!)
        }
        context.json(result)
    } catch (e: StatusRuntimeException) {
        when (e.status.code) {
            Status.Code.UNAVAILABLE -> throw ErrorStatusException(503, "Connection is currently not available.")
            else -> throw ErrorStatusException(500, "Failed to list locks for connection: ${e.message}")
        }
    }
}



