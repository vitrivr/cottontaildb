package ch.unibas.dmi.dbis.cottontail.database.general

import java.nio.file.Path

/**
 * A simple database object in Cottontail DB.
 *
 * Database objects are closeable. Furthermore, they have Cottontail DB specific attributes.
 */
interface DBO : AutoCloseable {
    /** The simple name  name of this [DBO]. */
    val name: String

    /** The fully qualified name of this [DBO]. */
    val fqn: String
        get() = if (parent?.name != null) { "${parent!!.name}/$name" } else { name }

    /** The parent DBO (if such exists). */
    val parent: DBO?

    /** The [Path] to the [DBO]'s main file OR folder. */
    val path: Path

    /** True if this [DBO] was closed, false otherwise. */
    val closed: Boolean
}