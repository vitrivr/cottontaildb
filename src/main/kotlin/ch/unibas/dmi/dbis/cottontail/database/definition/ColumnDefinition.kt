package ch.unibas.dmi.dbis.cottontail.database.definition

import java.nio.file.Path

class ColumnDefinition<T : Any>(val name: String, val path: Path, val type: ColumnType<T>)