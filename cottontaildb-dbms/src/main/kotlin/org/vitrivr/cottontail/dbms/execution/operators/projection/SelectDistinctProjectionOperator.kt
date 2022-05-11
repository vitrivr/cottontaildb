package org.vitrivr.cottontail.dbms.execution.operators.projection

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onCompletion
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.utilities.hashing.RecordHasher
import java.util.*

/**
 * An [Operator.PipelineOperator] used during query execution. It projects on the defined fields and makes sure, that the combination of specified [ColumnDef] remains unique.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class SelectDistinctProjectionOperator(parent: Operator, fields: List<Pair<Name.ColumnName, Boolean>>, config: Config) : Operator.PipelineOperator(parent) {

    /** Columns produced by [SelectDistinctProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = this.parent.columns.filter { c -> fields.any { f -> f.first == c.name }}

    /** The columns marked as distinct for this [SelectDistinctProjectionOperator]. */
    private val distinctColumns = this.parent.columns.filter { c -> fields.any { f -> f.first == c.name && f.second}}

    /** The name of the temporary [Store] used to execute this [SelectDistinctProjectionOperator] operation. */
    private val tmpPath = config.temporaryDataFolder().resolve("${UUID.randomUUID()}")

    /** The Xodus [Environment] used by Cottontail DB. This is an internal variable and not part of the official interface. */
    private val tmpEnvironment: Environment = Environments.newInstance(this.tmpPath.toFile(), config.xodus.toEnvironmentConfig())

    /** Start an exclusive transaction. */
    private val tmpTxn = this.tmpEnvironment.beginExclusiveTransaction()

    /** The [Store] used to execute this [SelectDistinctProjectionOperator] operation. */
    private val store: Store = this.tmpEnvironment.openStore("distinct", StoreConfig.WITHOUT_DUPLICATES, this.tmpTxn)

    /** [SelectProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val columns = this.columns.toTypedArray()
        val hasher = RecordHasher(this.distinctColumns)
        return this.parent.toFlow(context).mapNotNull { r ->
            /* Generates hash from record. */
            val hash = ArrayByteIterable(hasher.hash(r))

            /* If record could be added to list of seen records (i.e., is the first of its kind) then entry is returned. */
            if (this.store.add(this.tmpTxn, hash, r.tupleId.toKey())) {
                StandaloneRecord(r.tupleId, columns, Array(columns.size) { r[columns[it]]})
            } else {
                null
            }
        }.onCompletion {
            /* Abort transaction and close environment. */
            this@SelectDistinctProjectionOperator.tmpTxn.abort()
            this@SelectDistinctProjectionOperator.tmpEnvironment.close()
        }
    }
}