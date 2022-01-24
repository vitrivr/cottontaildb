package org.vitrivr.cottontail.core.queries.binding

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
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
     * Returns the [Value] for the given [Binding].
     *
     * @param binding The [Binding] to lookup.
     * @return The bound [Value].
     */
    operator fun get(binding: Binding): Value?

    /**
     * Returns the [Value] for the given [bindingIndex].
     *
     * @param bindingIndex The [Binding] to lookup.
     * @return The bound [Value].
     */
    operator fun get(bindingIndex: Int): Value?

    /**
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param value The [Value] to bind.
     * @param static True, if [Binding] is expected to change during query execution.
     * @return A value [Binding]
     */
    fun bind(value: Value, static: Boolean = true): Binding.Literal

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
     * @param value The [Value] to bind.
     * @return A value [Binding]
     */
    fun update(binding: Binding.Literal, value: Value?)

    /**
     * Creates and returns a [Binding] for the given [ColumnDef].
     *
     * @param column The [ColumnDef] to bind.
     * @return [Binding.Column]
     */
    fun bind(column: ColumnDef<*>): Binding.Column

    /**
     * Updates the [Binding.Column]s based on the given [Record].
     *
     * @param record The [Record] to update the [Binding.Column] with.
     */
    fun bindRecord(record: Record)
}