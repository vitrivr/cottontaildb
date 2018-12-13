package ch.unibas.dmis.dbis.cottontail.database.definition

import java.nio.file.Path

data class ColumnDefinition<T : Any>(val name: String, val path: Path, val type: ColumnType<T>, val index: IndexDefinition<T> = IndexDefinition(IndexDefinition.Companion.IndexType.NoIndex)){
    init{
        index.columnDefinition = this
    }
}