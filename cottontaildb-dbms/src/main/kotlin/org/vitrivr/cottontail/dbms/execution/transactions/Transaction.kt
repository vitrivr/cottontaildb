package org.vitrivr.cottontail.dbms.execution.transactions

import jetbrains.exodus.env.Transaction
import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.execution.ExecutionContext
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.general.Tx
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 * A [Transaction] can be used to query and interact with a [Transaction].
 *
 * This is the view of a [Transaction] that is available to the operators that execute a query.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface Transaction: ExecutionContext, TransactionMetadata {

    /** The [TransactionManager] this [Transaction] belongs to. */
    val manager: TransactionManager

    /**
     * Schedules an [Operator] in the context of this [Transaction] and blocks, until execution has completed.
     *
     * @param operator The [Operator] to execute.
     * @return Resulting [Flow] of [Tuple]s
     */
    fun execute(operator: Operator): Flow<Tuple>

    /**
     * Commits this [Transaction] thus finalizing and persisting all operations executed so far.
     */
    fun commit()

    /**
     * Rolls back this [Transaction] thus reverting all operations executed so far.
     */
    fun rollback()

    /**
     * Tries to kill this [Transaction] interrupting all running queries.
     *
     * A call to this method is a best-effort attempt to stop all ongoing queries. After killing a transaction
     * successfully, all changes made through it are rolled back.
     */
    fun kill()

    /**
     * Obtains a [Tx] for the given [Schema].
     *
     * @param mode The [AccessMode] for the requested [Tx].
     * @return The resulting [CatalogueTx] or null
     */
    fun catalogueTx(mode: AccessMode): CatalogueTx

    /**
     * Obtains a [Tx] for the given [Schema].
     *
     * @param name The [Name.SchemaName] to create the [Tx] for.
     * @param mode The [AccessMode] for the requested [Tx].
     * @return The resulting [SchemaTx] or null
     */
    fun schemaTx(name: Name.SchemaName, mode: AccessMode): SchemaTx

    /**
     * Obtains a [Tx] for the given [Entity].
     *
     * @param name The [Name.EntityName] to create the [Tx] for.
     * @param mode The [AccessMode] for the requested [Tx].
     * @return The resulting [EntityTx]
     */
    fun entityTx(name: Name.EntityName, mode: AccessMode): EntityTx

    /**
     * Obtains a [Tx] for the given [Column].
     *
     * @param name The [Name.ColumnName] to create the [Tx] for.
     * @param mode The [AccessMode] for the requested [Tx].
     * @return The resulting [ColumnTx] or null
     */
    fun columnTx(name: Name.ColumnName, mode: AccessMode): ColumnTx<*>

    /**
     * Obtains a [Tx] for the given [Index].
     *
     * @param name The [Name.IndexName] to create the [Tx] for.
     * @param mode The [AccessMode] for the requested [Tx].
     * @return The resulting [ColumnTx] or null
     */
    fun indexTx(name: Name.IndexName, mode: AccessMode): IndexTx

    /**
     * Signals an [Event] to this [Transaction].
     *
     * This method is a facility to communicate actions that take place within a
     * [Transaction] to the 'outside' world. Usually, that communication
     * must be withheld until the [Transaction] commits.
     *
     * @param event The [Event] that has been reported.
     */
    fun signalEvent(event: Event)
}