package org.vitrivr.cottontail.dbms.exceptions

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.general.DBOVersion

open class DatabaseException(message: String, cause: Throwable? = null) : Throwable(message, cause) {
    /**
     * Thrown when trying to access a [DBO] with an older, unsupported [DBOVersion].
     *
     * @param expected: Int Expected [DBOVersion] of [DBO]
     * @param found: Int Found [DBOVersion] of [DBO]
     */
    class VersionMismatchException(val expected: DBOVersion, val found: DBOVersion) : DatabaseException("Version mismatch for DBO: Expected $expected but found $found.")

    /**
     * Thrown when trying to create a [Schema][org.vitrivr.cottontail.dbms.schema.DefaultSchema]
     * that does already exist.
     *
     * @param schema [Name] of the [Schema][org.vitrivr.cottontail.dbms.schema.DefaultSchema].
     */
    class SchemaAlreadyExistsException(val schema: Name.SchemaName) : DatabaseException("Schema '$schema' does already exist!")

    /**
     * Thrown when trying to access a [Schema][org.vitrivr.cottontail.dbms.schema.DefaultSchema]
     * that does not exist.
     *
     * @param schema [Name] of the [Schema][org.vitrivr.cottontail.dbms.schema.DefaultSchema].
     */
    class SchemaDoesNotExistException(val schema: Name.SchemaName) : DatabaseException("Schema '$schema' does not exist!")

    /**
     * Thrown when trying to create an [Entity][org.vitrivr.cottontail.dbms.entity.DefaultEntity]
     * that does already exist.
     *
     * @param entity [Name] of the [Entity][org.vitrivr.cottontail.dbms.entity.DefaultEntity].
     */
    class EntityAlreadyExistsException(val entity: Name.EntityName) : DatabaseException("Entity '$entity' does already exist!")

    /**
     * Thrown when trying to access an [Entity][org.vitrivr.cottontail.dbms.entity.DefaultEntity]
     * that does not exist.
     *
     * @param entity [Name] of the [Entity][org.vitrivr.cottontail.dbms.entity.DefaultEntity].
     */
    class EntityDoesNotExistException(val entity: Name.EntityName) : DatabaseException("Entity '$entity' does not exist!")

    /**
     * Thrown whenever trying to create an [Index][org.vitrivr.cottontail.dbms.index.AbstractIndex]
     * that does already exist.
     *
     * @param index The [Name] of the [Index][org.vitrivr.cottontail.dbms.index.AbstractIndex]
     */
    class IndexAlreadyExistsException(val index: Name.IndexName) : DatabaseException("Index '$index' does already exist!")

    /**
     * Thrown whenever trying to access an [Index][org.vitrivr.cottontail.dbms.index.AbstractIndex]
     * that does not exist.
     *
     * @param index The [Name] of the [Index][org.vitrivr.cottontail.dbms.index.AbstractIndex]
     */
    class IndexDoesNotExistException(val index: Name) : DatabaseException("Index '$index' does not exist!")

    /**
     * Thrown whenever trying to create an [Index][[org.vitrivr.cottontail.dbms.index.AbstractIndex] that is not supported (yet). *
     *
     * @param index The [Name] of the [Index][org.vitrivr.cottontail.dbms.index.AbstractIndex]
     */
    class IndexNotSupportedException(val index: Name.IndexName, val reason: String) : DatabaseException("Index '$index' could not be created: $reason")

    /**
     * Thrown upon creation of an [Entity] if the definition contains no column.
     *
     * @param entity [Name] of the affected [Entity][org.vitrivr.cottontail.dbms.entity.DefaultEntity]
     */
    class NoColumnException(entity: Name.EntityName) : DatabaseException("Entity '$entity' could not be created because it does not contain a column.")

    /**
     * Thrown upon creation of an [Entity] if the definition contains duplicate column names.
     *
     * @param entity [Name.EntityName] of the affected [Entity]
     * @param name [Name.ColumnName] of the duplicate [Column] in the definition.
     */
    class DuplicateColumnException(entity: Name.EntityName, name: Name.ColumnName) : DatabaseException("Entity '$entity' could not be created because it contains duplicate column names '$name'.")

    /**
     * Thrown whenever trying to access a [Column][org.vitrivr.cottontail.dbms.column.Column]
     * that does not exist.
     *
     * @param column The [Name] of the [Column][org.vitrivr.cottontail.dbms.column.Column].
     */
    class ColumnDoesNotExistException(val column: Name.ColumnName) : DatabaseException("Column $column does not exist.")

    /**
     * Thrown when the Cottontail DB engine expects a different type of file.
     *
     * @param type The name of the expected type.
     */
    class InvalidFileException(type: String) : DatabaseException("The provided file is not a valid $type file!")

    /**
     * Thrown when the Cottontail DB engine cannot read the data from a file OR the data read is unexpected. Usually, this can be attributed to data corruption
     *
     * @param message Description of the issue.
     */
    class DataCorruptionException(message: String) : DatabaseException(message)

    /**
     * Thrown when the Cottontail DB engine cannot write data because written value is reserved. Used mainly for nullable columns.
     *
     * @param message Description of the issue.
     */
    class ReservedValueException(message: String): DatabaseException(message)
}




