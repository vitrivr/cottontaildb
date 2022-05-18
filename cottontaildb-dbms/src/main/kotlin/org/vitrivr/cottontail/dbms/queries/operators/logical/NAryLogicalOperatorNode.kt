package org.vitrivr.cottontail.dbms.queries.operators.logical

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import java.io.PrintStream
import java.util.*

/**
 * An abstract [OperatorNode.Logical] implementation that has multiple [OperatorNode.Logical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.7.0
 */
abstract class NAryLogicalOperatorNode(vararg inputs: Logical): OperatorNode.Logical() {

    /** The inputs to this [NAryLogicalOperatorNode]. The first input belongs to the same group. */
    private val _inputs: MutableList<Logical> = LinkedList<Logical>()
    val inputs: List<Logical>
        get() = Collections.unmodifiableList(this._inputs)

    /** A [BinaryLogicalOperatorNode]'s index is always the [depth] of its first input + 1. */
    final override var depth: Int = 0
        private set

    /**
     * The group ID of a [NAryLogicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [NAryLogicalOperatorNode]s.
     */
    final override val groupId: GroupId
        get() = this._inputs.firstOrNull()?.groupId ?: 0

    /** The [base] of a [NAryLogicalOperatorNode] is always itself. */
    final override val base: Collection<Logical>
        get() = this._inputs.flatMap { it.base }

    /** By default, the [NAryLogicalOperatorNode] outputs the physical [ColumnDef] of its input. */
    override val physicalColumns: List<ColumnDef<*>>
        get() = (this.inputs.firstOrNull()?.physicalColumns ?: emptyList())

    /** By default, the [NAryLogicalOperatorNode] outputs the [ColumnDef] of its input. */
    override val columns: List<ColumnDef<*>>
        get() = (this.inputs.firstOrNull()?.columns ?: emptyList())

    /** By default, a [NAryLogicalOperatorNode] doesn't have any requirement. */
    override val requires: List<ColumnDef<*>>
        get() = emptyList()

    init {
        inputs.forEach { this.addInput(it) }
    }

    /**
     * Adds a [OperatorNode.Logical] to the list of inputs.
     *
     * @param input [OperatorNode.Logical] that should be added as input.
     */
    fun addInput(input: Logical) {
        require(input.output == null) { "Cannot connect $input to $this: Output is already occupied!" }
        if (this._inputs.size == 0) {
            this.depth = input.depth + 1
        }
        this._inputs.add(input)
        input.output = this
    }

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [NAryLogicalOperatorNode].
     */
    abstract override fun copy(): NAryLogicalOperatorNode

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] using the provided [OperatorNode.Logical] as input.
     *
     * @param input The [OperatorNode.Logical]s that act as input to this [NAryLogicalOperatorNode].
     * @return Copy of this [NAryLogicalOperatorNode] with new input.
     */
    final override fun copy(vararg input: Logical): NAryLogicalOperatorNode {
        require(input.size <= this.inputArity) { "Cannot provide more than ${this.inputArity} inputs for ${this.javaClass.simpleName}." }
        val copy = this.copy()
        input.forEach { copy.addInput(it) }
        return copy
    }

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] and its entire output [OperatorNode.Logical] tree using the provided nodes as input.
     *
     * @param input The [OperatorNode.Logical]s that act as input.
     * @return Copy of this [NAryLogicalOperatorNode] with its output.
     */
    final override fun copyWithOutput(vararg input: Logical): Logical {
        val copy = this.copy(*input)
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] and the entire input [OperatorNode.Logical] tree that belong to the same [GroupId].
     *
     * @return Copy of this [NAryLogicalOperatorNode].
     */
    final override fun copyWithGroupInputs(): NAryLogicalOperatorNode {
        val copy = this.copy()
        val input = this.inputs.firstOrNull()?.copyWithGroupInputs()
        if (input != null) {
            copy.addInput(input)
        }
        return copy
    }

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] and the entire input [OperatorNode.Logical] tree.
     *
     * @return Copy of this [NAryLogicalOperatorNode].
     */
    final override fun copyWithInputs(): NAryLogicalOperatorNode {
        val copy = this.copy()
        this.inputs.forEach { copy.addInput(it.copyWithInputs()) }
        return copy
    }

    /**
     * Calculates and returns the digest for this [NAryLogicalOperatorNode].
     *
     * @return [Digest] for this [NAryLogicalOperatorNode]
     */
    override fun digest(): Digest {
        var result = this.hashCode().toLong()
        for (i in this._inputs) {
            result = 33L * result + i.digest()
        }
        result += 33L * result + this.depth.hashCode()
        return result
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this._inputs.forEach { it.printTo(p) }
        super.printTo(p)
    }
}