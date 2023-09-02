package org.vitrivr.cottontail.dbms.index.hnsw

import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.dbms.index.basic.IndexConfig

class HnswIndexConfig(val maxItemCount: Int, val distance: VectorDistance<*>, val m: Int = 10, efConstruction: Int = 200, val ef: Int = 10): IndexConfig<HnswIndex> {



    override fun toMap(): Map<String, String> {
        TODO("Not yet implemented")
    }
}