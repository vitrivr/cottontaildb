package org.vitrivr.cottontail.dbms.execution.operators.sort

import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.toTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*

/**
 * An [Operator.PipelineOperator] used during query execution.
 *
 * Performs external sorting on the specified [ColumnDef]s and returns the [Tuple] in sorted order. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.10
 */
class ExternalMergeSortOperator(parent: Operator, sortOn: List<Pair<Binding.Column, SortOrder>>, private val chunkSize: Int, override val context: QueryContext) : AbstractSortOperator(parent, sortOn) {

    /** Temporary path used by this [ExternalMergeSortOperator]. */
    private val tmpPath = this@ExternalMergeSortOperator.context.catalogue.config.root.resolve("tmp").resolve(this@ExternalMergeSortOperator.context.queryId)

    /** A [LinkedList] of chunks created by this [ExternalMergeSortOperator]. */
    private val chunks = LinkedList<Path>()

    init {
        if (!Files.exists(this.tmpPath)) {
            Files.createDirectories(this.tmpPath)
        }
    }

    /**
     * Converts this [HeapSortOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [HeapSortOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val incoming = this@ExternalMergeSortOperator.parent.toFlow()
        val chunk = ArrayList<Tuple>(this@ExternalMergeSortOperator.chunkSize)

        /* Phase 1a: Read incoming tuples, sort them in chunks and write them to disk. */
        incoming.collect { tuple ->
            /* In-memory sort of chunk. */
            chunk.add(tuple)

            /* Write chunk to temporary file. */
            if (chunk.size >= this@ExternalMergeSortOperator.chunkSize) {
                this@ExternalMergeSortOperator.writeAndClear(chunk)
            }
        }

        /* Phase 1b: Write remainder (if exists) */
        if (chunk.size > 0) {
            this@ExternalMergeSortOperator.writeAndClear(chunk)
        }

        /* Phase 2a: Prepare input queue by reading one entry from every file. */
        val schema = this@ExternalMergeSortOperator.columns.toTypedArray()
        var counter = 0L
        val inputs = this@ExternalMergeSortOperator.chunks.associateWith { p -> Files.newInputStream(p, StandardOpenOption.READ) }.toMutableMap()
        val heap = ObjectHeapPriorityQueue<Pair<Tuple, Path>>(chunks.size) { a, b ->
            this@ExternalMergeSortOperator.comparator.compare(a.first, b.first)
        }

        try {
            /* Phase 2b: Populate input queue by reading one entry from every file. */
            for (i in inputs) {
                val tuple = CottontailGrpc.QueryResponseMessage.Tuple.parseDelimitedFrom(i.value).toTuple(counter++, schema)
                heap.enqueue(tuple to i.key)
            }

            /* Phase 2c: Now drain external files via the heap. */
            while (heap.size() > 0) {
                val next = heap.dequeue()
                val input = inputs[next.second]

                emit(next.first) /* Next record in queue. */
                if (input != null) {
                    val tuple = CottontailGrpc.QueryResponseMessage.Tuple.parseDelimitedFrom(input)?.toTuple(counter++, schema)
                    if (tuple != null) {
                        heap.enqueue(tuple to next.second)
                    } else {
                        input.close()
                        inputs.remove(next.second)
                    }
                }
            }
        } finally {
            inputs.forEach { it.value.close() } /* Close inputs. */
        }
    }.flowOn(Dispatchers.IO)


    /**
     * Writes a [MutableList] of [Tuple] to an external file.
     *
     * @param chunk The [MutableList] of [Tuple] to write.
     */
    private fun writeAndClear(chunk: MutableList<Tuple>) {
        val path = this.tmpPath.resolve("sort_${this.identifier}_${this.chunks.size}")
        chunk.sortWith(this@ExternalMergeSortOperator.comparator)
        Files.newOutputStream(path, StandardOpenOption.CREATE_NEW).use { os ->
            for (t in chunk) t.toTuple().writeDelimitedTo(os)
        }
        chunk.clear()
        this.chunks.add(path)
    }
}