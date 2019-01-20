package ch.unibas.dmi.dbis.cottontail.model.exceptions

import ch.unibas.dmi.dbis.cottontail.database.schema.ColumnType
import org.mapdb.DBException
import java.util.*

open class DatabaseException(message: String) : Throwable(message) {

    /** */
    class ColumnNotExistException(column: String, entity: String): DBException("Column '$column' does not exist on entity '$entity'.")

    /** */
    class ColumnTypeUnexpectedException(column: String, entity: String, expected: ColumnType<*>, actual: ColumnType<*>): DBException("Column '$column' on entity '$entity' has wrong type (expected: ${expected.name}, actual: ${actual.name}).")

    /** */
    class InvalidFileException(type: String): DBException("The provided file is not a valid $type file!")

    /** */
    class DataCorruptionException(message: String): DBException(message)
}




