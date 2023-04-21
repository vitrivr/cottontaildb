package org.vitrivr.cottontail.ui.api.session

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.ui.model.session.Connection
import java.util.concurrent.TimeUnit

/**
 * A [Session] as started by a user of the Thumper UI.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Session: AutoCloseable {

    companion object {
       const val SESSION_ATTRIBUTE_KEY = "SESSION"
    }


    /**
     * A [List] of all [ManagedChannel]s opened in this [Session].
     */
    private val connections = HashMap<Connection,ManagedChannel>()


    /**
     * Creates a new [ManagedChannel] for the provided [Connection] .
     *
     * @param connection The [Connection] to connect to.
     * @return True on success, false otherwise.
     */
    fun connect(connection: Connection): Boolean {
        return if (!this.connections.contains(connection)) {
            this.connections[connection] = ManagedChannelBuilder.forAddress(connection.host, connection.port).enableFullStreamDecompression().usePlaintext().build()
            true
        } else {
            false
        }
    }

    /**
     * Disconnects a [ManagedChannel] for the provided [host] and [port].
     *
     * @param connection The [Connection] to disconnect from.
     * @return True on success, false otherwise.
     */
    fun disconnect(connection: Connection): Boolean {
        return if (this.connections.contains(connection)) {
            this.connections[connection]?.shutdown()
            this.connections[connection]?.awaitTermination(1000, TimeUnit.MILLISECONDS)
            this.connections.remove(connection)
            true
        } else {
            false
        }
    }

    /**
     * Returns a [List] of all active [Connection]s.
     *
     * @return [List] of all active [Connection]s.
     */
    fun list() = this.connections.keys.toList()

    /**
     * Closes this [Session] and all connections associated with it.
     */
    override fun close() {
        this.connections.values.removeIf { c ->
            try {
                if (!c.isShutdown) {
                    c.shutdown()
                    c.awaitTermination(1000, TimeUnit.MILLISECONDS)
                }
            } catch (e: Throwable) {
                /* TODO: Log. */
            }
            true
        }
    }
}