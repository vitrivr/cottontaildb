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
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Analyses an [Entity] and updates statistics.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class AnalyseEntityOperator(private val tx: CatalogueTx, private val name: Name.EntityName) : AbstractDataDefinitionOperator(name, "ANALYSE ENTITY") {
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val schemaTxn = context.getTx(this@AnalyseEntityOperator.tx.schemaForName(this@AnalyseEntityOperator.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this@AnalyseEntityOperator.name)) as EntityTx
        val time = measureTimeMillis {
            /** Analyse all columns that have stale statistics. */
            for (column in entityTxn.listColumns().map { entityTxn.columnForName(it.name) }) {
                val columnTx = context.getTx(column) as ColumnTx<*>
                columnTx.analyse()
            }
        }
        emit(this@AnalyseEntityOperator.statusRecord(time))
    }
}