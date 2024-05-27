package org.vitrivr.cottontail.dbms.index.deg.serializer

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.dbms.index.deg.primitives.Node
import org.vitrivr.cottontail.utilities.graph.undirected.VertexSerializer
import java.io.ByteArrayInputStream

class TupleIdNodeSerializer: VertexSerializer<Node<TupleId>> {
    override fun write(vertex: Node<TupleId>, output: LightOutputStream) {
        LongBinding.writeCompressed(output, vertex.label)
    }

    override fun read(input: ByteArrayInputStream): Node<TupleId> = Node(
        LongBinding.readCompressed(input)
    )
}