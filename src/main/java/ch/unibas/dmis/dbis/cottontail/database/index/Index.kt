package ch.unibas.dmis.dbis.cottontail.database.index

import ch.unibas.dmis.dbis.cottontail.database.definition.IndexDefinition

sealed class Index<T>{

    abstract val type: IndexDefinition.Companion.IndexType

    /**
     * returns a list of all TIDs which contain this value or null
     */
    abstract fun getTIDs(value: T): List<Long>?


    /**
     * adds TIDs to index for specified value
     */
    abstract fun putTIDs(value: T, vararg tids: Long)

}

class NoIndex<T> : Index<T>() {
    override val type = IndexDefinition.Companion.IndexType.NoIndex

    override fun getTIDs(value: T): List<Long>? = null

    override fun putTIDs(value: T, vararg tids: Long){}
}