package ch.unibas.dmi.dbis.cottontail.model.exceptions

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef

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
    class ColumnDoesNotExistException(column: ColumnDef<*>): DatabaseException("The column $column does not exist!")

    /**
     *
     */
    class TypeException(): DatabaseException("The provided value is not of the expected type.")

    /**
     * This kind of exception is thrown whenever the query does not adhere to some syntax requirement.
     * I.e. mandatory components of the query are missing.
     *
     * @param message Message describing the issue.
     */
    class QuerySyntaxException(message: String): DatabaseException(message)

    /**
     * This kind of exception is thrown whenever a query fails to bind to a specific [DBO]. This is usually
     *  the case, if [Schema], [Entity] or [Column] names are not spelt correctly.
     *
     * @param message Message describing the issue with the query.
     */
    class QueryBindException(message: String): DatabaseException(message)

    /**
     * This kind of exception is thrown whenever a literal value cannot be cast to the desired value.
     *
     * @param message Message describing the issue with the query.
     */
    class UnsupportedCastException(message: String): DatabaseException(message)
}