package ch.unibas.dmi.dbis.cottontail.database.general

/**
 * A database object in Cottontail DB. Database objects are closeable. Furthermore, they have a status
 * with regards to being open or closed.
 */
interface DBO : AutoCloseable {
    /**
     * True if this [DBO] was closed, false otherwise.
     */
    val closed: Boolean
}