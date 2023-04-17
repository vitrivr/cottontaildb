package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.services.AutoAnalyzerService
import org.vitrivr.cottontail.dbms.execution.services.StatisticsManagerService
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Analyses an [Entity] and updates statistics.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class AnalyseEntityOperator(private val tx: CatalogueTx, private val name: Name.EntityName, private val service: StatisticsManagerService? = null, override val context: QueryContext) : AbstractDataDefinitionOperator(name, "ANALYSE ENTITY") {
    override fun toFlow(): Flow<Record> = flow {
        val schemaTxn = this@AnalyseEntityOperator.tx.schemaForName(this@AnalyseEntityOperator.name.schema()).newTx(this@AnalyseEntityOperator.context)
        val entityTxn = schemaTxn.entityForName(this@AnalyseEntityOperator.name).newTx(this@AnalyseEntityOperator.context)
        val time = measureTimeMillis {
            if (this@AnalyseEntityOperator.service != null) {
                this@AnalyseEntityOperator.service.updateStatisticsOfEntity(this@AnalyseEntityOperator.name)
                /*for (column in entityTxn.listColumns().map { entityTxn.columnForName(it.name) }) {
                    this@AnalyseEntityOperator.service.increaseChangeCount(column.name)
                }*/
            } else {
                for (column in entityTxn.listColumns().map { entityTxn.columnForName(it.name) }) {
                    val columnTx = column.newTx(this@AnalyseEntityOperator.context)
                    columnTx.analyse()
                }
            }
        }
        emit(this@AnalyseEntityOperator.statusRecord(time))
    }
}