package org.vitrivr.cottontail.ui.api.session

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.ui.model.session.Connection
import org.vitrivr.cottontail.ui.model.session.Session
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock
import kotlin.concurrent.read
import kotlin.concurrent.write


/**
 * A singleton instance that manages the different Cottontail DB connections for the Thumper UI.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ConnectionManager {

    /** The [ManagedChannel]s managed by this [ConnectionManager]. */
    private val connections: HashMap<String, LinkedHashMap<Connection,ManagedChannel>> = HashMap()

    /** A [StampedLock] to mediate access to [ConnectionManager].  */
    private val lock = ReentrantReadWriteLock()

    /**
     * Creates a new [ManagedChannel] for the provided [Connection] .
     *
     * @param connection The [Connection] to connect to.
     * @return True on success, false otherwise.
     */
    fun connect(session: Session, connection: Connection): Boolean = this.lock.write {
        val sessionStore = this.connections.computeIfAbsent(session.id) { LinkedHashMap() }
        return if (!sessionStore.contains(connection)) {
            sessionStore[connection] = ManagedChannelBuilder.forAddress(connection.host, connection.port).enableFullStreamDecompression().usePlaintext().build()
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
    fun disconnect(session: Session, connection: Connection): Boolean = this.lock.write {
        val sessionStore = this.connections[session.id] ?: return false
        val channel = sessionStore[connection] ?: return false
        channel.shutdown()
        channel.awaitTermination(1000, TimeUnit.MILLISECONDS)
        sessionStore.remove(connection)
        true
    }

    /**
     * Ends a [Session] and closes all associated [Connection]s.
     *
     * @param session The [Session] to end.
     */
    fun end(session: Session): Boolean = this.lock.write {
        val sessionStore = this.connections[session.id] ?: return false
        for (channel in sessionStore.values) {
            channel.shutdown()
            channel.awaitTermination(1000, TimeUnit.MILLISECONDS)
        }
        this.connections.remove(session.id)
        true
    }

    /**
     * Closes all [Session]s and all connections associated with it.
     */
    fun purge() = this.lock.write {
        for (session in this.connections.values) {
            for (channel in session) {
                channel.value.shutdown()
                channel.value.awaitTermination(1000, TimeUnit.MILLISECONDS)
            }
        }
        this.connections.clear()
    }

    /**
     * Returns a [ManagedChannel] for a specific [Connection] and [Session].
     *
     * @return [ManagedChannel]
     */
    fun get(session: Session, connection: Connection): Triple<Session,Connection,ManagedChannel> = this.lock.read {
        Triple(session, connection, this.connections[session.id]?.get(connection) ?: throw IllegalStateException("No channel for connection $connection in session $session."))
    }

    /**
     * Returns a [List] of all active [Connection]s.
     *
     * @return [List] of all active [Connection]s.
     */
    fun list(session: Session) = this.lock.read {
        this.connections[session.id]?.keys?.toList() ?: emptyList()
    }
}