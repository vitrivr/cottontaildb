package org.vitrivr.cottontail.dbms.queries.planning.nodes.physical

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.Node
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.dbms.queries.planning.nodes.OperatorNode
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.NAryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.merge.MergePhysicalOperator
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.statistics.entity.RecordStatistics
import java.io.PrintStream
import java.util.*

/**
 * An abstract [OperatorNode.Physical] implementation that has multiple [OperatorNode.Physical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.5.0
 */
abstract class NAryPhysicalOperatorNode(vararg inputs: Physical) : OperatorNode.Physical() {

    /** The inputs to this [NAryLogicalOperatorNode]. The first input belongs to the same group. */
    private val _inputs: MutableList<Physical> = LinkedList<Physical>()
    val inputs: List<Physical>
        get() = Collections.unmodifiableList(this._inputs)

    /** A [NAryPhysicalOperatorNode]'s index is always the [depth] of its first input + 1. */
    final override var depth: Int = 0
        private set

    /**
     * The group ID of a [NAryPhysicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryPhysicalOperatorNode]s.
     */
    final override val groupId: GroupId
        get() = this.inputs.firstOrNull()?.groupId ?: 0

    /** The [base] of a [NAryPhysicalOperatorNode] is the sum of its input's bases. */
    final override val base: Collection<Physical>
        get() = this.inputs.flatMap { it.base }

    /** The input arity of a [NAryPhysicalOperatorNode] depends on the number of inputs. */
    final override val inputArity: Int
        get() = this.inputs.size

    /** The [totalCost] of a [NAryPhysicalOperatorNode] is the sum of its own and its input cost. */
    final override val totalCost: Cost
        get() {
            var cost = this.cost
            for (i in inputs) {
                cost += i.totalCost
            }
            return cost
        }

    /** By default, a [NAryPhysicalOperatorNode]'s order is unspecified. */
    override val sortOn: List<Pair<ColumnDef<*>, SortOrder>>
        get() = emptyList()

    /** By default, a [NAryPhysicalOperatorNode]'s requirements are empty. */
    override val requires: List<ColumnDef<*>>
        get() =  emptyList()

    /** [NAryPhysicalOperatorNode]s are executable if all their inputs are executable. */
    override val executable: Boolean
        get() = (this.inputs.size == this.inputArity) && this.inputs.all { it.executable }

    /** [NAryPhysicalOperatorNode] usually cannot be partitioned. */
    override val canBePartitioned: Boolean = false

    /** By default, the [NAryPhysicalOperatorNode] outputs the physical [ColumnDef] of its input. */
    override val physicalColumns: List<ColumnDef<*>>
        get() = (this.inputs.firstOrNull()?.physicalColumns ?: emptyList())

    /** By default, the [NAryPhysicalOperatorNode] outputs the [ColumnDef] of its input. */
    override val columns: List<ColumnDef<*>>
        get() = (this.inputs.firstOrNull()?.columns ?: emptyList())

    /** By default, a [UnaryPhysicalOperatorNode]'s [RecordStatistics] is retained. */
    override val statistics: RecordStatistics
        get() = this.inputs.firstOrNull()?.statistics ?: RecordStatistics.EMPTY

    init {
        inputs.forEach { this.addInput(it) }
    }

    /**
     * Adds a [OperatorNode.Physical] to the list of inputs.
     *
     * @param input [OperatorNode.Physical] that should be added as input.
     */
    fun addInput(input: Physical) {
        require(input.output == null) { "Cannot connect $input to $this: Output is already occupied!" }
        if (this._inputs.size == 0) {
            this.depth = input.depth + 1
        }
        this._inputs.add(input)
        input.output = this
    }

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [NAryPhysicalOperatorNode].
     */
    abstract override fun copy(): NAryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] and all its inputs that belong to the same [GroupId],
     * up and until the base of the tree.
     *
     * @return Copy of this [NAryPhysicalOperatorNode].
     */
    final override fun copyWithGroupInputs(): NAryPhysicalOperatorNode {
        val copy = this.copy()
        val input = this.inputs.firstOrNull()?.copyWithGroupInputs()
        if (input != null) {
            copy.addInput(input)
        }
        return copy
    }

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] and all its inputs up and until the base of the tree.
     *
     * @return Copy of this [NAryPhysicalOperatorNode].
     */
    final override fun copyWithInputs(): NAryPhysicalOperatorNode {
        val copy = this.copy()
        this.inputs.forEach { copy.addInput(it.copyWithInputs()) }
        return copy
    }

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] with its output reaching down to the [root] of the tree.
     * Furthermore, connects the provided [input] to the copied [OperatorNode.Physical]s.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [NAryPhysicalOperatorNode] with its output.
     */
    override fun copyWithOutput(vararg input: Physical): Physical {
        val copy = this.copy()
        input.forEach { copy.addInput(it) }
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * By default, [NAryPhysicalOperatorNode] cannot be partitioned.
     *
     * @param partitions The desired number of partitions.
     * @param p The partition index.
     * @return null
     */
    override fun tryPartition(partitions: Int, p: Int?): Physical? = null

    /**
     * By default, the [NAryPhysicalOperatorNode] simply propagates [bind] calls to its inpus.
     *
     * However, some implementations must propagate the call to inner [Node]s.
     *
     * @param context The [BindingContext] to bind this [NAryPhysicalOperatorNode] to.
     */
    override fun bind(context: BindingContext) {
        this.inputs.forEach { it.bind(context) }
    }

    /**
     * Calculates and returns the digest for this [BinaryPhysicalOperatorNode].
     *
     * @return [Digest] for this [BinaryPhysicalOperatorNode]
     */
    override fun digest(): Digest {
        var result = this.hashCode().toLong()
        for (i in this.inputs) {
            result += 27L * result + i.digest()
        }
        result += 27L * result + this.depth.hashCode()
        return result
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.inputs.forEach { it.printTo(p) }
        super.printTo(p)
    }
}