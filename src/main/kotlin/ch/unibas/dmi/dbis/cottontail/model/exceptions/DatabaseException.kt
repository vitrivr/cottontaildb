package ch.unibas.dmi.dbis.cottontail.model.exceptions

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import org.mapdb.DBException

open class DatabaseException(message: String) : Throwable(message) {

    /** */
    class SchemaAlreadyExistsException(schema: String): DatabaseException("Schema '$schema' does already exist!")

    /** */
    class SchemaDoesNotExistException(schema: String): DatabaseException("Schema '$schema' does not exist!")

    /** */
    class EntityAlreadyExistsException(schema: String, entity: String): DatabaseException("Entity '$entity' does already exist in schema '$schema'!")

    /** */
    class EntityDoesNotExistException(schema: String, entity: String): DatabaseException("Entity '$entity' does not exist in schema '$schema'!")

    /**
     * Thrown upon creation of an [Entity] if the definition contains duplicate column names.
     *
     * @param schema Name of the affected [Schema]
     * @param entity Name of the affected [Entity]
     * @param columns Name of the [Column]s in the definition.
     */
    class DuplicateColumnException(schema: String, entity: String, columns: String): DatabaseException("Entity $schema.$entity contains duplicate column names: $columns")

    /** */
    class ColumnNotExistException(column: String, entity: String): DatabaseException("Column '$column' does not exist on entity '$entity'.")

    /** */
    class ColumnTypeUnexpectedException(column: String, entity: String, expected: ColumnType<*>, actual: ColumnType<*>): DatabaseException("Column '$column' on entity '$entity' has wrong type (expected: ${expected.name}, actual: ${actual.name}).")

    /** */
    class InvalidFileException(type: String): DatabaseException("The provided file is not a valid $type file!")

    /** */
    class DataCorruptionException(message: String): DatabaseException(message)


    class ValidationException(message: String): DatabaseException(message)
}




