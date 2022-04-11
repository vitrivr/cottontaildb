package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.IndexState
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Optimizes an [Entity]
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class OptimizeEntityOperator(private val tx: CatalogueTx, private val name: Name.EntityName, private val force: Boolean = false) : AbstractDataDefinitionOperator(name, "OPTIMIZE ENTITY") {
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val schemaTxn = context.getTx(this@OptimizeEntityOperator.tx.schemaForName(this@OptimizeEntityOperator.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this@OptimizeEntityOperator.name)) as EntityTx
        val time = measureTimeMillis {
            /** Analyse all columns that have stale statistics. */
            for (column in entityTxn.listColumns().map { entityTxn.columnForName(it.name) }) {
                val columnTx = context.getTx(column) as ColumnTx<*>
                if (this@OptimizeEntityOperator.force || !columnTx.statistics().fresh) {
                    columnTx.analyse()
                }
            }

            /** Rebuild all indexes that are not CLEAN. */
            for (index in entityTxn.listIndexes().map { entityTxn.indexForName(it) }) {
                val indexTx = context.getTx(index) as IndexTx
                if (this@OptimizeEntityOperator.force || indexTx.state != IndexState.CLEAN) {
                    indexTx.rebuild()
                }
            }
        }
        emit(this@OptimizeEntityOperator.statusRecord(time))
    }
}