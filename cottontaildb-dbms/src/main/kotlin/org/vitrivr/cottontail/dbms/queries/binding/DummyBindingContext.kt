package org.vitrivr.cottontail.dbms.queries.binding

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A dummy [BindingContext] implementation used by certain operators that don't require value binding.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object DummyBindingContext: BindingContext {
    override fun get(binding: Binding.Literal): Value? = null
    override fun get(binding: Binding.Column): Value? = null
    override fun get(binding: Binding.Function): Value? = null
    override fun bind(value: Value, static: Boolean): Binding.Literal = throw UnsupportedOperationException("EmptyBindingContext does not support binding values.")
    override fun bind(column: ColumnDef<*>): Binding.Column = throw UnsupportedOperationException("EmptyBindingContext does not support binding columns.")
    override fun bind(function: Function<*>, arguments: List<Binding>): Binding.Function = throw UnsupportedOperationException("EmptyBindingContext does not support binding columns.")
    override fun update(binding: Binding.Literal, value: Value?) = throw UnsupportedOperationException("EmptyBindingContext does not support binding values.")
    override fun update(binding: Binding.Column, value: Value?) = throw UnsupportedOperationException("EmptyBindingContext does not support binding values.")
    override fun bindNull(type: Types<*>, static: Boolean): Binding.Literal = throw UnsupportedOperationException("EmptyBindingContext does not support binding values.")
    override fun copy() = DummyBindingContext
}