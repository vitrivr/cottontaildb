package org.vitrivr.cottontail.dbms.execution.operators.basics

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import java.util.*

/**
 * An [Operator] used during query execution and processing.
 *
 * @author Ralph Gasser
 * @version 1.8.0
 */
sealed class Operator {

    /** The [QueryContext] this [Operator] is executed in. */
    abstract val context: QueryContext

    /** The [GroupId] of this [Operator]. */
    abstract val groupId: GroupId

    /** The list of [ColumnDef]s produced by this [Operator]. */
    abstract val columns: List<ColumnDef<*>>

    /** An [String] that can be used to identify this [Operator]. */
    val identifier = UUID.randomUUID().toString().subSequence(0, 7)

    /**
     * Converts this [Operator] to a [Flow] and returns it.
     *
     * @return Resulting [Flow]
     */
    abstract fun toFlow(): Flow<Tuple>

    /**
     * An [Operator] that can be pipelined, i.e., has a parent [Operator] and no materialization of
     * intermediate results is required.
     *
     * @author Ralph Gasser
     * @version 1.6.0
     */
    abstract class PipelineOperator(val parent: Operator) : Operator() {
        /** Flag indicating whether this [PipelineOperator] acts as a pipeline breaker. */
        abstract val breaker: Boolean

        /** The [GroupId] of a [PipelineOperator] is inherited by the parent [Operator]. */
        override val groupId: GroupId
            get() = this.parent.groupId
    }

    /**
     * An [Operator] that can be pipelined and has multiple, incoming parent [Operator]s.
     *
     * @author Ralph Gasser
     * @version 1.6.0
     */
    abstract class MergingPipelineOperator(val parents: List<Operator>) : Operator() {
        init {
            if (this.parents.size <= 1) {
                throw OperatorSetupException(this, "The use of an MergingPipelineOperator requires at least two inputs.")
            }
        }

        /** Flag indicating whether this [MergingPipelineOperator] acts as a pipeline breaker. */
        abstract val breaker: Boolean

        /** The [GroupId] of a [SinkOperator] is inherited by the left most parent [Operator]. */
        override val groupId: GroupId
            get() = this.parents[0].groupId
    }

    /**
     * An [Operator] that acts as a sink, i.e., processes and consumes [Tuple]s.
     *
     * @author Ralph Gasser
     * @version 1.6.0
     */
    abstract class SinkOperator(val parent: Operator) : Operator() {
        final override val columns: List<ColumnDef<*>> = emptyList()

        /** The [GroupId] of a [SinkOperator] is inherited by the parent [Operator]. */
        override val groupId: GroupId
            get() = this.parent.groupId
    }

    /**
     * An [Operator] that acts as a source, i.e., thus has no parent [Operator].
     *
     * @author Ralph Gasser
     * @version 1.6.0
     */
    abstract class SourceOperator(override val groupId: GroupId = 0) : Operator()
}