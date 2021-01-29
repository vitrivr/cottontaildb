package org.vitrivr.cottontail.database.events

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An internal [DataChangeEvent] to signal changes made to an [Entity].
 *
 * @version 1.0.0
 * @author Ralph Gasser
 */
sealed class DataChangeEvent(
    val name: Name.EntityName,
    val tupleId: TupleId,
    val columns: Array<ColumnDef<*>>
) {

    /**
     * A [DataChangeEvent] that signals a INSERT into an [Entity]
     */
    class InsertDataChangeEvent(
        name: Name.EntityName,
        tupleId: TupleId,
        columns: Array<ColumnDef<*>>,
        val new: Array<Value?>
    ) : DataChangeEvent(name, tupleId, columns)

    /**
     * A [DataChangeEvent] that signals n UPDATE in an [Entity]
     */
    class UpdateDataChangeEvent(
        name: Name.EntityName,
        tupleId: TupleId,
        columns: Array<ColumnDef<*>>,
        val new: Array<Value?>,
        val old: Array<Value?>
    ) : DataChangeEvent(name, tupleId, columns)

    /**
     * A [DataChangeEvent] that signals a DELETE from an [Entity]
     */
    class DeleteDataChangeEvent(
        name: Name.EntityName,
        tupleId: TupleId,
        columns: Array<ColumnDef<*>>,
        val old: Array<Value?>
    ) : DataChangeEvent(name, tupleId, columns)
}