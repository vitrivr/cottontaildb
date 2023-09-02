package org.vitrivr.cottontail.dbms.index.hnsw


import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.index.hnsw.Murmur3.hash32
import java.io.*
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.concurrent.locks.ReentrantLock
import kotlin.math.abs
import kotlin.math.ln

/**
 * Implementation of [InMemoryIndex] that implements the hnsw algorithm.
 *
 * @param <TupleId>       Type of the external identifier of an item
 * @param <VectorValue<*>>   Type of the vector to perform distance calculation on
 * @param <TItem>     Type of items stored in the index
 * @param <Double> Type of distance between items (expect any numeric type: float, double, int, ..)
 * @see [
 * Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs](https://arxiv.org/abs/1603.09320)
 *
 * Implementation adapted from https://github.com/jelmerk/hnswlib
 *
 */
class InMemoryHnswIndex(
    /**
     * Returns the maximum number of items the index can hold.
     */
    private var maxItemCount: Int,
    private val distanceFunction: VectorDistance<*>,
    private val distanceComparator: Comparator<DoubleValue> = Comparator.naturalOrder(),
    /**
     * Returns the number of bi-directional links created for every new element during construction.
     */
    private val m: Int = 10,
    /**
     * Returns the parameter has the same meaning as ef, but controls the index time / index precision.
     */
    efConstruction: Int = 200,
    /**
     * The size of the dynamic list for the nearest neighbors (used during the search)
     */
    private val ef: Int = 10,
    private var isRemoveEnabled: Boolean = false


) {


    private val maxValueDistanceComparator: MaxValueComparator<DoubleValue> = MaxValueComparator(distanceComparator)

    /**
     * Returns the dimensionality of the items stored in this index.
     *
     * @return the dimensionality of the items stored in this index
     */


    /**
     * Returns the number of bi-directional links created for every new element during construction.
     *
     * @return the number of bi-directional links created for every new element during construction
     */

    private val maxM: Int = m
    private val maxM0: Int = m * 2
    private val levelLambda: Double = 1 / ln(m.toDouble())
    /**
     * The size of the dynamic list for the nearest neighbors (used during the search)
     *
     * @return The size of the dynamic list for the nearest neighbors
     */
    /**
     * Set the size of the dynamic list for the nearest neighbors (used during the search)
     *
     * @param ef The size of the dynamic list for the nearest neighbors
     */


    /**
     * Returns the parameter has the same meaning as ef, but controls the index time / index precision.
     *
     * @return the parameter has the same meaning as ef, but controls the index time / index precision
     */
    private val efConstruction: Int = efConstruction.coerceAtLeast(m)


    private var nodeCount = 0

    @Volatile
    private var entryPoint: Node? = null
    private var nodes: AtomicReferenceArray<Node?> = AtomicReferenceArray(maxItemCount)
    private val lookup: MutableMap<TupleId, Int> = mutableMapOf()

    private val locks: MutableMap<TupleId, Any> = HashMap()

    private val globalLock: ReentrantLock = ReentrantLock()
    private var visitedBitSetPool: GenericObjectPool<ArrayBitSet>
    private var excludedCandidates: ArrayBitSet
    fun contains(id: TupleId): Boolean {
        return get(id) != null
    }


    @Throws(InterruptedException::class)
    fun addAll(items: Collection<Pair<TupleId, VectorValue<*>>>) {
        addAll(items, Runtime.getRuntime().availableProcessors())
    }

    @Throws(InterruptedException::class)
    fun addAll(
        items: Collection<Pair<TupleId, VectorValue<*>>>,
        numThreads: Int,
    ) {
        val executorService = ThreadPoolExecutor(
            numThreads, numThreads, 60L, TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(),
            NamedThreadFactory("indexer-%d")
        )
        executorService.allowCoreThreadTimeOut(true)
        try {
            val queue: Queue<Pair<TupleId, VectorValue<*>>> = LinkedBlockingQueue(items)
            val futures: MutableList<Future<*>> = ArrayList()
            for (threadId in 0 until numThreads) {
                futures.add(executorService.submit {
                    while (true) {
                        val item = queue.poll() ?: break
                        add(item)
                    }
                })
            }
            for (future in futures) {
                try {
                    future.get()
                } catch (e: ExecutionException) {
                    throw RuntimeException("An exception was thrown by one of the threads.", e.cause)
                }
            }
        } finally {
            executorService.shutdown()
        }
    }

    fun findNeighbors(id: TupleId, k: Int): List<SearchResult<Pair<TupleId, VectorValue<*>>, DoubleValue>> {
        return get(id)?.let { item: Pair<TupleId, VectorValue<*>> ->
            findNearest(
                item.second, k + 1
            ).asSequence()
                .filter { result: SearchResult<Pair<TupleId, VectorValue<*>>, DoubleValue> ->
                    result.item().first != id
                }
                .take(k)
                .toList()
        } ?: emptyList()
    }

    init {
        visitedBitSetPool = GenericObjectPool(
            { ArrayBitSet(maxItemCount) },
            Runtime.getRuntime().availableProcessors()
        )
        excludedCandidates = ArrayBitSet(maxItemCount)
    }

    /**
     * {@inheritDoc}
     */
    fun size(): Int {
        globalLock.lock()
        return try {
            lookup.size
        } finally {
            globalLock.unlock()
        }
    }

    /**
     * {@inheritDoc}
     */
    fun get(id: TupleId): Pair<TupleId, VectorValue<*>>? {
        globalLock.lock()
        return try {
            val nodeId = lookup[id] ?: return null
            nodes[nodeId]!!.item
        } finally {
            globalLock.unlock()
        }
    }

    /**
     * {@inheritDoc}
     */
    fun items(): Collection<Pair<TupleId, VectorValue<*>>> {
        globalLock.lock()
        return try {
            val results: MutableList<Pair<TupleId, VectorValue<*>>> = ArrayList(size())
            val iter: Iterator<Pair<TupleId, VectorValue<*>>> = ItemIterator()
            while (iter.hasNext()) {
                results.add(iter.next())
            }
            results
        } finally {
            globalLock.unlock()
        }
    }

    /**
     * {@inheritDoc}
     */
    fun remove(id: TupleId): Boolean {
        if (!isRemoveEnabled) {
            return false
        }
        globalLock.lock()
        return try {
            val internalNodeId = lookup[id] ?: return false
            val node = nodes[internalNodeId]!!
            node.deleted = true
            lookup.remove(id)
            true
        } finally {
            globalLock.unlock()
        }
    }

    /**
     * {@inheritDoc}
     */
    fun add(item: Pair<TupleId, VectorValue<*>>): Boolean {
        val randomLevel = assignLevel(item.first, levelLambda)
        val connections = arrayOfNulls<MutableList<Int>>(randomLevel + 1)
        for (level in 0..randomLevel) {
            val levelM = if (randomLevel == 0) maxM0 else maxM
            connections[level] = ArrayList(levelM)
        }
        globalLock.lock()
        try {
            val existingNodeId = lookup[item.first]
            if (existingNodeId != null) {
                if (!isRemoveEnabled) {
                    return false
                }
                val node = nodes[existingNodeId]!!
                if (Objects.deepEquals(node.item.second, item.second)) {
                    node.item = item
                    return true
                } else {
                    remove(item.first)
                }
            }
            if (nodeCount >= maxItemCount) {
                throw SizeLimitExceededException("The number of elements exceeds the specified limit.")
            }
            val newNodeId = nodeCount++
            synchronized(excludedCandidates) { excludedCandidates.add(newNodeId) }
            val newNode = Node(newNodeId, connections, item, false)
            nodes[newNodeId] = newNode
            lookup[item.first] = newNodeId
            val lock = locks.computeIfAbsent(item.first) { k: TupleId -> Any() }
            val entryPointCopy = entryPoint
            try {
                synchronized(lock) {
                    synchronized(newNode) {
                        if (entryPoint != null && randomLevel <= entryPoint!!.maxLevel()) {
                            globalLock.unlock()
                        }
                        var currObj = entryPointCopy
                        if (currObj != null) {
                            if (newNode.maxLevel() < entryPointCopy!!.maxLevel()) {
                                var curDist = distanceFunction.invoke(item.second, currObj.item.second)
                                for (activeLevel in entryPointCopy.maxLevel() downTo newNode.maxLevel() + 1) {
                                    var changed = true
                                    while (changed) {
                                        changed = false
                                        synchronized(currObj!!) {
                                            val candidateConnections = currObj!!.connections[activeLevel]
                                            for (i in 0 until candidateConnections!!.size) {
                                                val candidateId = candidateConnections[i]
                                                val candidateNode = nodes[candidateId]
                                                val candidateDistance = distanceFunction.invoke(
                                                    item.second,
                                                    candidateNode!!.item.second
                                                )
                                                if (lt(candidateDistance, curDist)) {
                                                    curDist = candidateDistance
                                                    currObj = candidateNode
                                                    changed = true
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            for (level in randomLevel.coerceAtMost(entryPointCopy.maxLevel()) downTo 0) {
                                val topCandidates = searchBaseLayer(currObj, item.second, efConstruction, level)
                                if (entryPointCopy.deleted) {
                                    val distance =
                                        distanceFunction.invoke(item.second, entryPointCopy.item.second)
                                    topCandidates.add(
                                        NodeIdAndDistance(
                                            entryPointCopy.id, distance, maxValueDistanceComparator
                                        )
                                    )
                                    if (topCandidates.size > efConstruction) {
                                        topCandidates.poll()
                                    }
                                }
                                mutuallyConnectNewElement(newNode, topCandidates, level)
                            }
                        }

                        // zoom out to the highest level
                        if (entryPoint == null || newNode.maxLevel() > entryPointCopy!!.maxLevel()) {
                            // this is thread safe because we get the global lock when we add a level
                            entryPoint = newNode
                        }
                        return true
                    }
                }
            } finally {
                synchronized(excludedCandidates) { excludedCandidates.remove(newNodeId) }
            }
        } finally {
            if (globalLock.isHeldByCurrentThread) {
                globalLock.unlock()
            }
        }
    }

    private fun mutuallyConnectNewElement(
        newNode: Node,
        topCandidates: PriorityQueue<NodeIdAndDistance<Double>>,
        level: Int
    ) {
        val bestN = if (level == 0) maxM0 else maxM
        val newNodeId = newNode.id
        val newItemVector = newNode.item.second
        val newItemConnections = newNode.connections[level]
        getNeighborsByHeuristic2(topCandidates, m)
        while (!topCandidates.isEmpty()) {
            val selectedNeighbourId = topCandidates.poll().nodeId
            newItemConnections!!.add(selectedNeighbourId)
            val neighbourNode = nodes[selectedNeighbourId]
            synchronized(neighbourNode!!) {
                val neighbourVector = neighbourNode.item.second
                val neighbourConnectionsAtLevel = neighbourNode.connections[level]
                if (neighbourConnectionsAtLevel!!.size < bestN) {
                    neighbourConnectionsAtLevel.add(newNodeId)
                } else {
                    // finding the "weakest" element to replace it with the new one
                    val dMax = distanceFunction.invoke(
                        newItemVector,
                        neighbourNode.item.second
                    )
                    val comparator = Comparator
                        .naturalOrder<NodeIdAndDistance<Double>>().reversed()
                    val candidates = PriorityQueue(comparator)
                    candidates.add(NodeIdAndDistance(newNodeId, dMax, maxValueDistanceComparator))
                    neighbourConnectionsAtLevel.forEach { id: Int ->
                        val dist = distanceFunction.invoke(
                            neighbourVector,
                            nodes[id]!!.item.second
                        )
                        candidates.add(NodeIdAndDistance(id, dist, maxValueDistanceComparator))
                    }
                    getNeighborsByHeuristic2(candidates, bestN)
                    neighbourConnectionsAtLevel.clear()
                    while (!candidates.isEmpty()) {
                        neighbourConnectionsAtLevel.add(candidates.poll().nodeId)
                    }
                }
            }
        }
    }

    private fun getNeighborsByHeuristic2(topCandidates: PriorityQueue<NodeIdAndDistance<Double>>, m: Int) {
        if (topCandidates.size < m) {
            return
        }
        val queueClosest = PriorityQueue<NodeIdAndDistance<Double>>()
        val returnList: MutableList<NodeIdAndDistance<Double>> = ArrayList()
        while (!topCandidates.isEmpty()) {
            queueClosest.add(topCandidates.poll())
        }
        while (!queueClosest.isEmpty()) {
            if (returnList.size >= m) {
                break
            }
            val currentPair = queueClosest.poll()
            val distToQuery = currentPair.distance
            var good = true
            for (secondPair in returnList) {
                val curdist = distanceFunction.invoke(
                    nodes[secondPair.nodeId]!!.item.second,
                    nodes[currentPair.nodeId]!!.item.second
                )
                if (lt(curdist, distToQuery)) {
                    good = false
                    break
                }
            }
            if (good) {
                returnList.add(currentPair)
            }
        }
        topCandidates.addAll(returnList)
    }

    /**
     * {@inheritDoc}
     */
    fun findNearest(
        vector: VectorValue<*>,
        k: Int
    ): List<SearchResult<Pair<TupleId, VectorValue<*>>, DoubleValue>> {
        if (entryPoint == null) {
            return emptyList()
        }
        val entryPointCopy: Node = entryPoint!!
        var currObj: Node? = entryPointCopy
        var curDist = distanceFunction.invoke(vector, currObj!!.item.second)
        for (activeLevel in entryPointCopy.maxLevel() downTo 1) {
            var changed = true
            while (changed) {
                changed = false
                synchronized(currObj!!) {
                    val candidateConnections = currObj!!.connections[activeLevel]
                    for (i in 0 until candidateConnections!!.size) {
                        val candidateId = candidateConnections[i]
                        val candidateDistance = distanceFunction.invoke(
                            vector,
                            nodes[candidateId]!!.item.second
                        )
                        if (lt(candidateDistance, curDist)) {
                            curDist = candidateDistance
                            currObj = nodes[candidateId]
                            changed = true
                        }
                    }
                }
            }
        }
        val topCandidates = searchBaseLayer(
            currObj, vector, ef.coerceAtLeast(k), 0
        )
        while (topCandidates.size > k) {
            topCandidates.poll()
        }
        val results: ArrayList<SearchResult<Pair<TupleId, VectorValue<*>>, DoubleValue>> = ArrayList(topCandidates.size)
        while (!topCandidates.isEmpty()) {
            val pair = topCandidates.poll()
            results.add(
                0,
                SearchResult(
                    nodes[pair.nodeId]!!.item,
                    pair.distance!!,
                    maxValueDistanceComparator
                )
            )
        }
        return results
    }

    /**
     * Changes the maximum capacity of the index.
     * @param newSize new size of the index
     */
    fun resize(newSize: Int) {
        globalLock.lock()
        try {
            maxItemCount = newSize
            visitedBitSetPool = GenericObjectPool(
                { ArrayBitSet(maxItemCount) },
                Runtime.getRuntime().availableProcessors()
            )
            val newNodes = AtomicReferenceArray<Node?>(newSize)
            for (i in 0 until nodes.length()) {
                newNodes[i] = nodes[i]
            }
            nodes = newNodes
            excludedCandidates = ArrayBitSet(excludedCandidates, newSize)
        } finally {
            globalLock.unlock()
        }
    }

    private fun searchBaseLayer(
        entryPointNode: Node?, destination: VectorValue<*>, k: Int, layer: Int
    ): PriorityQueue<NodeIdAndDistance<Double>> {
        val visitedBitSet = visitedBitSetPool.borrowObject()
        return try {
            val topCandidates = PriorityQueue(Comparator.naturalOrder<NodeIdAndDistance<Double>>().reversed())
            val candidateSet = PriorityQueue<NodeIdAndDistance<Double>>()
            var lowerBound: DoubleValue?
            if (!entryPointNode!!.deleted) {
                val distance = distanceFunction.invoke(destination, entryPointNode.item.second)
                val pair = NodeIdAndDistance<Double>(
                    entryPointNode.id, distance, maxValueDistanceComparator
                )
                topCandidates.add(pair)
                lowerBound = distance
                candidateSet.add(pair)
            } else {
                lowerBound = MaxValueComparator.maxValue()
                val pair = NodeIdAndDistance<Double>(
                    entryPointNode.id, lowerBound, maxValueDistanceComparator
                )
                candidateSet.add(pair)
            }
            visitedBitSet.add(entryPointNode.id)
            while (!candidateSet.isEmpty()) {
                val currentPair = candidateSet.poll()
                if (gt(currentPair.distance, lowerBound)) {
                    break
                }
                val node = nodes[currentPair.nodeId]
                synchronized(node!!) {
                    val candidates = node.connections[layer]
                    for (i in 0 until candidates!!.size) {
                        val candidateId = candidates[i]
                        if (!visitedBitSet.contains(candidateId)) {
                            visitedBitSet.add(candidateId)
                            val candidateNode = nodes[candidateId]
                            val candidateDistance = distanceFunction.invoke(
                                destination,
                                candidateNode!!.item.second
                            )
                            if (topCandidates.size < k || gt(lowerBound, candidateDistance)) {
                                val candidatePair = NodeIdAndDistance<Double>(
                                    candidateId,
                                    candidateDistance,
                                    maxValueDistanceComparator
                                )
                                candidateSet.add(candidatePair)
                                if (!candidateNode.deleted) {
                                    topCandidates.add(candidatePair)
                                }
                                if (topCandidates.size > k) {
                                    topCandidates.poll()
                                }
                                if (!topCandidates.isEmpty()) {
                                    lowerBound = topCandidates.peek().distance
                                }
                            }
                        }
                    }
                }
            }
            topCandidates
        } finally {
            visitedBitSet.clear()
            visitedBitSetPool.returnObject(visitedBitSet)
        }
    }


    private fun assignLevel(value: TupleId, lambda: Double): Int {

        // by relying on the external id to come up with the level, the graph construction should be a lot mor stable
        // see : https://github.com/nmslib/hnswlib/issues/28
        val hashCode = value.hashCode()
        val bytes = byteArrayOf(
            (hashCode shr 24).toByte(),
            (hashCode shr 16).toByte(),
            (hashCode shr 8).toByte(),
            hashCode.toByte()
        )
        val random = abs(hash32(bytes).toDouble() / Int.MAX_VALUE.toDouble())
        val r = -ln(random) * lambda
        return r.toInt()
    }

    private fun lt(x: DoubleValue?, y: DoubleValue?): Boolean {
        return maxValueDistanceComparator.compare(x, y) < 0
    }

    private fun gt(x: DoubleValue?, y: DoubleValue?): Boolean {
        return maxValueDistanceComparator.compare(x, y) > 0
    }

    internal inner class ItemIterator : Iterator<Pair<TupleId, VectorValue<*>>> {
        private var done = 0
        private var index = 0
        override fun hasNext(): Boolean {
            return done < size()
        }

        override fun next(): Pair<TupleId, VectorValue<*>> {
            var node: Node?
            do {
                node = nodes[index++]
            } while (node == null || node.deleted)
            done++
            return node.item
        }

    }

    internal class Node(
        val id: Int,
        val connections: Array<MutableList<Int>?>,
        @field:Volatile var item: Pair<TupleId, VectorValue<*>>,
        @field:Volatile var deleted: Boolean
    ) {
        fun maxLevel(): Int {
            return connections.size - 1
        }
    }

    internal class NodeIdAndDistance<Double>(
        val nodeId: Int,
        val distance: DoubleValue?,
        private val distanceComparator: Comparator<DoubleValue>
    ) : Comparable<NodeIdAndDistance<Double>> {
        override fun compareTo(other: NodeIdAndDistance<Double>): Int {
            return distanceComparator.compare(distance, other.distance)
        }
    }

    internal class MaxValueComparator<Double>(private val delegate: Comparator<Double>) : Comparator<Double>{
        override fun compare(o1: Double?, o2: Double?): Int {
            return if (o1 == null) if (o2 == null) 0 else 1 else if (o2 == null) -1 else delegate.compare(o1, o2)
        }

        companion object {
            fun <Double> maxValue(): Double? {
                return null
            }
        }
    }

}