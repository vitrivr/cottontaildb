package org.vitrivr.cottontail.core.queries.binding

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A context for late binding of values. Late binding can be used during query planning or execution, e.g., when
 * literal values are replaced upon re-use of a query plan or when values used in query execution are dynamically loaded.
 *
 * The [BindingContext] class is NOT thread-safe. When concurrently executing part of a query plan, different copies of
 * the [BindingContext] should be created and used.
 *
 * @author Ralph Gasser
 * @version 1.1.0
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
     * Returns the [Value] for the given [Binding.Column].
     *
     * @param binding The [Binding.Column] to lookup.
     * @return The bound [Value].
     */
    operator fun get(binding: Binding.Column): Value?

    /**
     * Returns the [Value] for the given [Binding.Function].
     *
     * @param binding The [Binding.Function] to lookup.
     * @return The bound [Value].
     */
    operator fun get(binding: Binding.Function): Value?

    /**
     * Creates and returns a [Binding.Literal] for the given [Value].
     *
     * @param value The [Value] to bind.
     * @param static True, if [Binding] is expected to change during query execution.
     * @return A value [Binding]
     */
    fun bind(value: Value, static: Boolean = true): Binding.Literal

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
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param type The [Types] to bind.
     * @param static True, if [Binding] is expected to change during query execution.
     * @return A value [Binding]
     */
    fun bindNull(type: Types<*>, static: Boolean = true): Binding.Literal

    /**
     * Updates the [Value] for a [Binding.Literal].
     *
     * @param binding The [Binding.Literal] to update.
     * @param value The new [Value] to bind.
     */
    fun update(binding: Binding.Literal, value: Value?)

    /**
     * Updates the [Value] for a [Binding.Column].
     *
     * @param binding The [Binding.Column] to update.
     * @param value The new [Value] to bind.
     */
    fun update(binding: Binding.Column, value: Value?)

    /**
     * Creates a copy of this [BindingContext]. The copy must be fully independent.
     *
     * @return Copy of this [BindingContext]
     */
    fun copy(): BindingContext
}