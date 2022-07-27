package org.vitrivr.cottontail.dbms.queries.operators.basics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Logical] implementation that has multiple [OperatorNode.Logical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
abstract class NAryLogicalOperatorNode(vararg inputs: Logical): OperatorNode.Logical() {

    /** The inputs to this [NAryLogicalOperatorNode]. The first input belongs to the same group. */
    val inputs: List<Logical> = inputs.toList()

    /** A [NAryLogicalOperatorNode]'s index is always the [depth] of its first input + 1. */
    final override var depth: Int = 0
        private set

    /**
     * The group ID of a [NAryLogicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [NAryLogicalOperatorNode]s.
     */
    final override val groupId: GroupId = this.inputs[0].groupId

    /** The [base] of a [NAryLogicalOperatorNode] is always the base of its inputs. */
    final override val base: List<Logical> by lazy {
        this.inputs.flatMap { it.base }
    }

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
        this.inputs.forEach { it.output = this }
    }

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] using the provided nodes as input.
     *
     * @param input The [OperatorNode.Logical]s that act as input to this [NAryLogicalOperatorNode].
     * @return Copy of this [NAryLogicalOperatorNode] with new input.
     */
    abstract override fun copyWithNewInput(vararg input: Logical): NAryLogicalOperatorNode

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] and its entire output [OperatorNode.Logical] tree using the provided nodes as input.
     *
     * @param input The [OperatorNode.Logical]s that act as input.
     * @return Copy of this [NAryLogicalOperatorNode] with its output.
     */
    final override fun copyWithOutput(vararg input: Logical): Logical {
        val copy = this.copyWithNewInput(*input)
        this.output?.copyWithOutput(copy)
        return copy
    }

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] and the entire input [OperatorNode.Logical] tree that belong to the same [GroupId].
     *
     * @return Copy of this [NAryLogicalOperatorNode].
     */
    final override fun copyWithExistingGroupInput(vararg replacements: Logical): NAryLogicalOperatorNode {
        require(replacements.size == this.inputArity - 1) { "The input arity for NAryLogicalOperatorNode.copyWithGroupInputs() must be (${this.inputArity -1}) but is ${replacements.size}. This is a programmer's error!"}
        return this.copyWithNewInput(this.inputs.first().copyWithExistingInput(), *replacements)
    }

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] and the entire input [OperatorNode.Logical] tree.
     *
     * @return Copy of this [NAryLogicalOperatorNode].
     */
    final override fun copyWithExistingInput(): NAryLogicalOperatorNode {
        return this.copyWithNewInput(*this.inputs.map { it.copyWithExistingInput() }.toTypedArray())
    }

    /**
     * Calculates and returns the digest for this [NAryLogicalOperatorNode].
     *
     * @return [Digest] for this [NAryLogicalOperatorNode]
     */
    override fun digest(): Digest {
        var result = this.hashCode().toLong()
        for (i in this.inputs) {
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
        this.inputs.forEach { it.printTo(p) }
        super.printTo(p)
    }
}