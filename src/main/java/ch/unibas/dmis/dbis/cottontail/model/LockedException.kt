package ch.unibas.dmis.dbis.cottontail.model

class LockedException(message: String, vararg args: Any) : DatabaseException(message, args)
