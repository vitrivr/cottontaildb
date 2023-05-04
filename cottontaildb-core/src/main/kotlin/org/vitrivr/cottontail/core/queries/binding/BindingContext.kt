package org.vitrivr.cottontail.core.queries.binding

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value

/**
 * A context for late binding of values. Late binding can be used during query planning or execution, e.g., when
 * literal values are replaced upon re-use of a query plan or when values used in query execution are dynamically loaded.
 *
 * The [BindingContext] class is NOT thread-safe. When concurrently executing part of a query plan, different copies of
 * the [BindingContext] should be created and used.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface BindingContext {

    /**
     * Returns the [Value] for the given [Binding.Literal].
     *
     * @param binding The [Binding.Literal] to lookup.
     * @return The bound [Value].
     */
    operator fun get(binding: Binding.Literal): Value?

    /**
     * Returns the [Value] for the given [Binding.LiteralList].
     *
     * @param binding The [Binding.LiteralList] to lookup.
     * @return [List] of bound [Value]s.
     */
    operator fun get(binding: Binding.LiteralList): List<Value?>


    /**
     * Returns the [Value] for the given [Binding.Function].
     *
     * @param binding The [Binding.Function] to lookup.
     * @return The bound [Value].
     */
    context(Record)
    operator fun get(binding: Binding.Function): Value?

    /**
     * Returns the [Value]s for the given [Binding.Subquery].
     *
     * @param binding The [Binding.Subquery] to lookup.
     * @return A [List] of the bound [Value]s.
     */
    operator fun get(binding: Binding.Subquery): List<Value?>

    /**
     * Creates and returns a [Binding.Literal] for the given [Value].
     *
     * @param value The [Value] to bind.
     * @param static True for [Binding.Literal] that cannot be updated.
     * @return A value [Binding.Literal]
     */
    fun bind(value: Value, static: Boolean = true): Binding.Literal

    /**
     * Creates and returns a [Binding.LiteralList] for the given [List] of [Value]s.
     *
     * @param values The [List] of [Value]s to bind.
     * @return A value [Binding.LiteralList]
     */
    fun bind(values: List<Value>): Binding.LiteralList

    /**
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param type The [Types] to bind.
     * @param static True for [Binding.Literal] that cannot be updated.
     * @return [Binding.Literal]
     */
    fun bindNull(type: Types<*>, static: Boolean = true): Binding.Literal

    /**
     * Creates and returns a [Binding.Column] for the given [ColumnDef].
     *
     * @param column The [ColumnDef] to bind.
     * @return [Binding.Column]
     */
    fun bind(column: ColumnDef<*>): Binding.Column

    /**
     * Creates and returns a [Binding.Function] for the given [Function] invocation
     *
     * @param function The [Function] to bind.
     * @param arguments The list of argument [Binding]s for the [Function] invocation
     * @return [Binding.Function]
     */
    fun bind(function: Function<*>, arguments: List<Binding> = emptyList()): Binding.Function

    /**
     * Creates and returns a [Binding.Subquery] for the given [GroupId] invocation
     *
     * @param dependsOn The [GroupId] of the query plan that generates the values.
     * @param column The[ColumnDef] that should be extracted from the results.
     * @return [: Binding.Subquery]
     */
    fun bind(dependsOn: GroupId, column: ColumnDef<*>): Binding.Subquery

    /**
     * Appends a [Value] for a [Binding.Subquery].
     *
     * @param binding The [Binding.Subquery] to append value to.
     * @param value The new [Value] to append.
     */
    fun append(binding: Binding.Subquery, value: Value)

    /**
     * Clears all [Value]s for a [Binding.Subquery].
     *
     * @param binding The [Binding.Subquery] to clear.
     */
    fun clear(binding: Binding.Subquery)

    /**
     * Updates the [Value] for a [Binding.Literal].
     *
     * @param binding The [Binding.Literal] to update.
     * @param value The new [Value] to bind.
     */
    fun update(binding: Binding.Literal, value: Value?)

    /**
     * Creates a copy of this [BindingContext]. The copy must be fully independent.
     *
     * @return Copy of this [BindingContext]
     */
    fun copy(): BindingContext
}