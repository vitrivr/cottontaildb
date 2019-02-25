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
    class EntityDoesNotExistException(fqn: String): DatabaseException("Entity '$fqn' does not exist!")

    /**
     * Thrown whenever trying to create and [Index] that does already exist.
     *
     * @param fqn The FQN of the [Index]
     */
    class IndexAlreadyExistsException(fqn: String): DatabaseException("Index '$fqn' does already exist!")

    /**
     * Thrown whenever trying to access and [Index] that does not exist.
     *
     * @param fqn The FQN of the [Index]
     */
    class IndexDoesNotExistException(fqn: String): DatabaseException("Index '$fqn' does not exist!")

    /**
     * Thrown upon creation of an [Entity] if the definition contains duplicate column names.
     *
     * @param fqn Name of the affected [Entity]
     * @param columns Name of the [Column]s in the definition.
     */
    class DuplicateColumnException(fqn: String, columns: String) : DatabaseException("Entity '$fqn' could not be created because it contains duplicate column names (c=[$columns])!")

    /**
     * Throws by [Index]es if they are given a [Predicate] they cannot executed.
     *
     * @param schema Name of the affected [Schema]
     * @param entity Name of the affected [Entity]
     * @param name Name of the affected [Index].
     */
    class PredicateNotSupportedBxIndexException(schema: String, entity: String, index: String): DatabaseException("Index '$schema.$entity.$index' cannot be used to executed given predicate.")

    /** */
    class ColumnNotExistException(column: String, entity: String): DatabaseException("Column '$column' does not exist on entity '$entity'.")

    /** */
    class ColumnTypeUnexpectedException(column: String, entity: String, expected: ColumnType<*>, actual: ColumnType<*>): DatabaseException("Column '$column' on entity '$entity' has wrong type (expected: ${expected.name}, actual: ${actual.name}).")

    /** */
    class InvalidFileException(type: String): DatabaseException("The provided file is not a valid $type file!")

    /** */
    class DataCorruptionException(message: String): DatabaseException(message)
}




