package org.vitrivr.cottontail.dbms.queries.binding

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A dummy [BindingContext] implementation used by certain operators that don't require value binding.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object EmptyBindingContext: BindingContext {
    override fun get(binding: Binding): Value? = null
    override fun get(bindingIndex: Int): Value? = null
    override fun bind(value: Value): Binding.Literal {
        throw UnsupportedOperationException("EmptyBindingContext does not support binding values.")
    }
    override fun bind(column: ColumnDef<*>): Binding.Column {
        throw UnsupportedOperationException("EmptyBindingContext does not support binding values.")
    }
    override fun update(binding: Binding.Literal, value: Value?) {
        throw UnsupportedOperationException("EmptyBindingContext does not support updating values.")
    }
    override fun bindNull(type: Types<*>): Binding.Literal {
        throw UnsupportedOperationException("EmptyBindingContext does not support binding values.")
    }
    override fun bindRecord(record: Record) {
        throw UnsupportedOperationException("EmptyBindingContext does not support binding values.")
    }
}