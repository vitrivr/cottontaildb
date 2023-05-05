package org.vitrivr.cottontail.ui.api.database

import io.javalin.http.Context
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.ui.api.session.ConnectionManager
import org.vitrivr.cottontail.ui.model.session.Connection
import org.vitrivr.cottontail.ui.model.session.Session
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException
import java.util.LinkedList

/**
 * Obtains a [SimpleClient] given the current [Session] and the connection details encoded in the URL.
 *
 * @return [SimpleClient]
 */
fun Context.obtainClientForContext(): SimpleClient {
    val session = this.sessionAttribute<Session>(Session.USER_SESSION_KEY) ?: throw ErrorStatusException(400, "No active session found. Connection could not be established.")
    val connection = try {
        ConnectionManager.get(session, Connection.parse(this.pathParam("connection")))
    } catch (e: Throwable) {
        throw ErrorStatusException(400, "Connection could not be obtained. Did you connect to the database?")
    }
    return SimpleClient(connection.third)
}

/**
 * Drains a [TupleIterator] to a [List] using a user-defined mapping function.
 *
 * @param mapping The mapping function to build the list with.
 * @return [List] of mapped elements.
 */
fun <R> TupleIterator.drainToList(mapping: (Tuple) -> R): List<R> {
    val result = LinkedList<R>()
    while (this.hasNext()) {
        result.add(mapping(this.next()))
    }
    return result
}