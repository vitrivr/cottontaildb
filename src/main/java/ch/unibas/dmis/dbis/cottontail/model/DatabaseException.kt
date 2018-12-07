package ch.unibas.dmis.dbis.cottontail.model

open class DatabaseException(message: String, vararg args: Any) : Throwable(String.format(message, *args))
