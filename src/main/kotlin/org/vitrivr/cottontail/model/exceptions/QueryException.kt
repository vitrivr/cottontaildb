package org.vitrivr.cottontail.model.exceptions

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name

/**
 * These exceptions are thrown whenever a query cannot be executed properly.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
open class QueryException(message: String) : DatabaseException(message) {

    /**
     *
     */
    class ColumnDoesNotExistException(column: ColumnDef<*>) : DatabaseException("The column $column does not exist!")

    /**
     *
     */
    class TypeException : DatabaseException("The provided value is not of the expected type.")

    /**
     * This kind of exception is thrown whenever the query does not adhere to some syntax requirement.
     * I.e. mandatory components of the query are missing.
     *
     * @param message Message describing the issue.
     */
    class QuerySyntaxException(message: String) : DatabaseException(message)

    /**
     * This kind of exception is thrown whenever a query fails to bind to a specific [DBO][org.vitrivr.cottontail.database.general.DBO].
     * This is usually the case, if [Schema][org.vitrivr.cottontail.database.schema.Schema],
     * [Entity][org.vitrivr.cottontail.database.entity.Entity] or [Column][org.vitrivr.cottontail.database.column.Column]
     * names are not spelt correctly.
     *
     * @param message Message describing the issue with the query.
     */
    class QueryBindException(message: String) : DatabaseException(message)

    /**
     * This kind of exception is thrown whenever a literal value cannot be cast to the desired value.
     *
     * @param message Message describing the issue with the query.
     */
    class UnsupportedCastException(message: String) : DatabaseException(message)

    /**
     * This kind of exception is thrown whenever a [Predicate][org.vitrivr.cottontail.database.queries.Predicate]
     * is applied that is not supported by the data structure it is supplied to.
     *
     * @param message Message describing the issue with the query.
     */
    class UnsupportedPredicateException(message: String) : DatabaseException(message)

    /**
     * This kind of exception is thrown whenever a [Predicate][org.vitrivr.cottontail.database.queries.Predicate]
     * is routed through an [Index][org.vitrivr.cottontail.database.index.Index]
     * that does not support that kind of [Predicate][org.vitrivr.cottontail.database.queries.Predicate].
     *
     * @param index FQN of the index.
     * @param message Error message
     */
    class IndexLookupFailedException(index: Name, message: String) : QueryException("Lookup through index '$index' failed: $message")
}