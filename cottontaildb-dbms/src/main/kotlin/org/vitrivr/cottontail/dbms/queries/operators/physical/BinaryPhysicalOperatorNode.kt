package org.vitrivr.cottontail.dbms.queries.operators.physical

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.Node
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Physical] implementation that has exactly two [OperatorNode.Physical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.5.0
 */
abstract class BinaryPhysicalOperatorNode(left: Physical? = null, right: Physical? = null) : org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical() {

    /** Input arity of [BinaryPhysicalOperatorNode] is always two. */
    final override val inputArity: Int = 2

    /** A [BinaryPhysicalOperatorNode]'s index is always the [depth] of its [left] input + 1. This is set in the [left]'s setter. */
    final override var depth: Int = 0
        private set

    /**
     * The group ID of a [BinaryPhysicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryPhysicalOperatorNode]s.
     */
    final override val groupId: GroupId
        get() = this.left?.groupId ?: 0

    /** The left branch of the input; belongs to the same group! */
    var left: Physical? = null
        set(value) {
            require(value?.output == null) { "Cannot connect $value to $this: Output is already occupied!" }
            field?.output = null
            value?.output = this
            this.depth = value?.depth?.plus(1) ?: 0
            field = value
        }

    /** The right branch of the input; constitutes a new group! */
    var right: Physical? = null
        set(value) {
            require(value?.output == null) { "Cannot connect $value to $this: Output is already occupied!" }
            field?.output = null
            value?.output = this
            field = value
        }

    /** The [base] of a [BinaryPhysicalOperatorNode] is the sum of its input's bases. */
    final override val base: Collection<Physical>
        get() = (this.left?.base ?: emptyList()) +
                (this.right?.base ?: emptyList())

    /** The [totalCost] of a [BinaryPhysicalOperatorNode] is always the sum of its own and its input cost. */
    final override val totalCost: Cost
        get() = (this.left?.totalCost ?: Cost.ZERO) +
                (this.right?.totalCost ?: Cost.ZERO) + this.cost

    /** By default, [BinaryPhysicalOperatorNode]s are executable if both their inputs are executable. */
    override val executable: Boolean
        get() = (this.left?.executable ?: false) && (this.right?.executable ?: false)

    /** By default, [BinaryPhysicalOperatorNode]s cannot be partitioned. */
    override val canBePartitioned: Boolean = false

    /** By default, the [BinaryPhysicalOperatorNode] outputs the physical [ColumnDef] of its left input. */
    override val physicalColumns: List<ColumnDef<*>>
        get() = (this.left?.physicalColumns ?: emptyList())

    /** By default, the [BinaryPhysicalOperatorNode] outputs the [ColumnDef] of its left input. */
    override val columns: List<ColumnDef<*>>
        get() = (this.left?.columns ?: emptyList())

    /** By default, the output size of a [UnaryPhysicalOperatorNode] is the same as its left input's output size. */
    override val outputSize: Long
        get() = (this.left?.outputSize ?: 0)

    /** By default, a [BinaryPhysicalOperatorNode]'s order is inherited from the left branch of of the tree. */
    override val sortOn: List<Pair<ColumnDef<*>, SortOrder>>
        get() = this.left?.sortOn ?: emptyList()

    /** By default, a [BinaryPhysicalOperatorNode]'s has no specific requirements. */
    override val requires: List<ColumnDef<*>>
        get() = emptyList()

    /** By default, a [BinaryPhysicalOperatorNode]'s statistics are retained from its left input. */
    override val statistics: Map<ColumnDef<*>,ValueStatistics<*>>
        get() = this.left?.statistics ?: emptyMap()

    init {
        this.left = left
        this.right = right
    }

    /**
     * Creates and returns a copy of this [BinaryPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [BinaryPhysicalOperatorNode].
     */
    abstract override fun copy(): BinaryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [BinaryPhysicalOperatorNode] and all its inputs that belong to the same [GroupId],
     * up and until the base of the tree.
     *
     * @return Copy of this [BinaryPhysicalOperatorNode].
     */
    final override fun copyWithGroupInputs(): BinaryPhysicalOperatorNode {
        val copy = this.copy()
        copy.left = this.left?.copyWithGroupInputs()
        return copy
    }

    /**
     * Creates and returns a copy of this [BinaryPhysicalOperatorNode] and all its inputs up and until the base of the tree.
     *
     * @return Copy of this [BinaryPhysicalOperatorNode].
     */
    final override fun copyWithInputs(): BinaryPhysicalOperatorNode {
        val copy = this.copyWithGroupInputs()
        copy.right = this.right?.copyWithInputs()
        return copy
    }

    /**
     * Creates and returns a copy of this [BinaryPhysicalOperatorNode] with its output reaching down to the [root] of the tree.
     * Furthermore connects the provided [input] to the copied [OperatorNode.Physical]s.
     *
     * @param input The [OperatorNode.Logical]s that act as input.
     * @return Copy of this [OperatorNode.Logical] with its output.
     */
    override fun copyWithOutput(vararg input: Physical): Physical {
        require(input.size <= this.inputArity) { "Cannot provide more than ${this.inputArity} inputs for ${this.javaClass.simpleName}." }
        val copy = this.copy()
        copy.left = input.getOrNull(0)
        copy.right = input.getOrNull(1)
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * By default, the [BinaryPhysicalOperatorNode] simply propagates [bind] calls to its inpus.
     *
     * However, some implementations must propagate the call to inner [Node]s.
     *
     * @param context The [BindingContext] to bind this [BinaryPhysicalOperatorNode] to.
     */
    override fun bind(context: BindingContext) {
        this.left?.bind(context)
        this.right?.bind(context)
    }

    /**
     * Calculates and returns the digest for this [BinaryPhysicalOperatorNode].
     *
     * @return [Digest] for this [BinaryPhysicalOperatorNode]
     */
    override fun digest(): Digest {
        var result = 27L * hashCode() + (this.left?.digest() ?: -1L)
        result = 27L * result + (this.right?.digest() ?: -1L)
        return 27L * result + this.depth.hashCode()
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.left?.printTo(p)
        this.right?.printTo(p)
        super.printTo(p)
    }
}