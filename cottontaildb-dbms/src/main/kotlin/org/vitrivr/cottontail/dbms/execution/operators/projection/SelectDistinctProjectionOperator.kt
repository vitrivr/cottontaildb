package org.vitrivr.cottontail.dbms.execution.operators.projection

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.utilities.hashing.RecordHasher
import java.util.*

/**
 * An [Operator.PipelineOperator] used during query execution. It projects on the defined fields and makes sure, that the combination of specified [ColumnDef] remains unique.
 *
 * Only produces a single [Tuple].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class SelectDistinctProjectionOperator(parent: Operator, fields: List<Pair<Name.ColumnName, Boolean>>, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** Columns produced by [SelectDistinctProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = this.parent.columns.filter { c -> fields.any { f -> f.first == c.name }}

    /** The columns marked as distinct for this [SelectDistinctProjectionOperator]. */
    private val distinctColumns = this.parent.columns.filter { c -> fields.any { f -> f.first == c.name && f.second}}

    /** The name of the temporary [Store] used to execute this [SelectDistinctProjectionOperator] operation. */
    private val tmpPath = this.context.catalogue.config.temporaryDataFolder().resolve("${UUID.randomUUID()}")

    /** The Xodus [Environment] used by Cottontail DB. This is an internal variable and not part of the official interface. */
    private val tmpEnvironment: Environment = Environments.newInstance(this.tmpPath.toFile(), this.context.catalogue.config.xodus.toEnvironmentConfig())

    /** Start an exclusive transaction. */
    private val tmpTxn = this.tmpEnvironment.beginExclusiveTransaction()

    /** The [Store] used to execute this [SelectDistinctProjectionOperator] operation. */
    private val store: Store = this.tmpEnvironment.openStore("distinct", StoreConfig.WITHOUT_DUPLICATES, this.tmpTxn)

    /** [SelectProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val columns = this@SelectDistinctProjectionOperator.columns.toTypedArray()
        val hasher = RecordHasher(this@SelectDistinctProjectionOperator.distinctColumns)
        val incoming = this@SelectDistinctProjectionOperator.parent.toFlow()
        incoming.collect { r ->
            /* Generates hash from record. */
            val hash = ArrayByteIterable(hasher.hash(r))

            /* If record could be added to list of seen records (i.e., is the first of its kind) then entry is returned. */
            if (this@SelectDistinctProjectionOperator.store.add(this@SelectDistinctProjectionOperator.tmpTxn, hash, r.tupleId.toKey())) {
                emit(StandaloneTuple(r.tupleId, columns, Array(columns.size) { r[columns[it]]}))
            }
        }
    }.onCompletion {
        /* Abort transaction and close environment. */
        this@SelectDistinctProjectionOperator.tmpTxn.abort()
        this@SelectDistinctProjectionOperator.tmpEnvironment.close()
    }
}