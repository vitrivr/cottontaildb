package ch.unibas.dmis.dbis.cottontail.database.definition

import java.nio.file.Path

class IndexDefinition<T: Any>(val type: IndexType) {

    private var _columnDefinition : ColumnDefinition<T>? = null

    var columnDefinition: ColumnDefinition<T>
        get() = this.columnDefinition!!
        internal set(columnDefinition) {this._columnDefinition = columnDefinition}

    companion object {
        enum class IndexType{
            NoIndex
        }
    }

}