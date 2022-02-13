package org.vitrivr.cottontail.core.database

/**
 * A [Name] that identifies a DBO used within Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class Name: Comparable<Name> {

    companion object {
        /* Delimiter between name components. */
        const val NAME_COMPONENT_DELIMITER = "."

        /* Character used for wildcards. */
        const val NAME_COMPONENT_WILDCARD = "*"

        /* Root name component in Cottontail DB names. */
        const val NAME_COMPONENT_ROOT = "warren"
    }

    /** Internal array of [Name] components. */
    abstract val components: Array<String>

    /** Returns the last component of this [Name], i.e. the simple name. */
    val simple: String
        get() = this.components.last()

    /** Returns the [RootName] reference. */
    val root: RootName
        get() = RootName

    /** Returns true if this [Name] matches the other [Name]. */
    abstract fun matches(other: Name): Boolean

    /**
     * Compares this [Name] to the other [Name]. Returns zero if this object is equal to
     * the specified other object, a negative number if it's less than other, or a positive
     * number if it's greater than other.
     *
     * @return Hash code for this [Name] object
     */
    override fun compareTo(other: Name): Int {
        for ((i, c) in this.components.withIndex()) {
            if (other.components.size > i) {
                val comp = c.compareTo(other.components[i])
                if (comp != 0) return comp
            } else {
                return 1
            }
        }
        return 0
    }

    /**
     * The [RootName] which always is 'warren'.
     */
    object RootName : Name() {
        override val components: Array<String> = arrayOf(NAME_COMPONENT_ROOT)
        override fun matches(other: Name): Boolean = (other == this)
        fun schema(name: String) = SchemaName(NAME_COMPONENT_ROOT, name)
        override fun equals(other: Any?): Boolean = other == this
        override fun hashCode(): Int = 42 + this.components.contentHashCode()
        override fun toString(): String = this.components.joinToString(NAME_COMPONENT_DELIMITER)
    }

    /**
     * A [Name] object used to identify a function in Cottontail DB.
     */
    data class FunctionName(override val components: Array<String>): Name() {

        constructor(functionName: String): this(arrayOf(NAME_COMPONENT_ROOT, functionName.lowercase()))

        init {
            require(this.components.size == 2) { "Function name $this is malformed. This is a programmer's error!"}
            require(this.components[0] == NAME_COMPONENT_ROOT) { "${this.components.joinToString(".")} is not a valid function name." }
        }

        /**
         * Checks for a match with another [Name]. Only exact matches, i.e. equality, are possible for [SchemaName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)

        override fun equals(other: Any?): Boolean = other is FunctionName && other.components.contentEquals(this.components)
        override fun hashCode(): Int = 42 + this.components.contentHashCode()
        override fun toString(): String = this.components.joinToString(NAME_COMPONENT_DELIMITER)
    }

    /**
     * A [Name] object used to identify a schema in Cottontail DB.
     */
    data class SchemaName(override val components: Array<String>): Name(){

        constructor(functionName: String): this(arrayOf(NAME_COMPONENT_ROOT, functionName.lowercase()))
        constructor(root: String, functionName: String): this(arrayOf(root.lowercase(), functionName.lowercase()))

        init {
            require(this.components.size == 2) { "Schema name $this is malformed. This is a programmer's error!"}
            require(this.components[0] == NAME_COMPONENT_ROOT) { "${this.components.joinToString(".")} is not a valid schema name." }
        }

        /**
         * Generates an [EntityName] as child of this [SchemaName].
         *
         * @param name Name of the [EntityName]
         * @return [EntityName]
         */
        fun entity(name: String) = EntityName(this.components[0], this.components[1], name)

        /**
         * Checks for a match with another [Name]. Only exact matches, i.e. equality, are possible for [SchemaName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)

        override fun equals(other: Any?): Boolean = other is SchemaName && other.components.contentEquals(this.components)
        override fun hashCode(): Int = 42 + this.components.contentHashCode()
        override fun toString(): String = this.components.joinToString(NAME_COMPONENT_DELIMITER)
    }


    /**
     * A [Name] object used to identify a sequence.
     */
    data class SequenceName(override val components: Array<String>) : Name() {
        constructor(schemaName: String, entityName: String, sequenceName: String): this(arrayOf(NAME_COMPONENT_ROOT, schemaName.lowercase(), entityName.lowercase(), sequenceName.lowercase()))
        constructor(root: String, schemaName: String, entityName: String, sequenceName: String): this(arrayOf(root.lowercase(), schemaName.lowercase(), entityName.lowercase(), sequenceName.lowercase()))

        /**
         * Returns parent [SchemaName] of this [SequenceName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName = SchemaName(this.components.copyOfRange(0, 2))

        /**
         * Returns parent [EntityName] of this [SequenceName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName = EntityName(this.components.copyOfRange(0, 3))

        /**
         * Checks for a match with another name. Only exact matches, i.e. equality, are possible for [SequenceName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)

        override fun equals(other: Any?): Boolean = other is SequenceName && other.components.contentEquals(this.components)
        override fun hashCode(): Int = 42 + this.components.contentHashCode()
        override fun toString(): String = this.components.joinToString(NAME_COMPONENT_DELIMITER)
    }

    /**
     * A [Name] object used to identify an entity in Cottontail DB.
     */
    data class EntityName(override val components: Array<String>): Name() {

        constructor(schemaName: String, entityName: String): this(arrayOf(NAME_COMPONENT_ROOT, schemaName.lowercase(), entityName.lowercase()))
        constructor(root: String, schemaName: String, entityName: String): this(arrayOf(root.lowercase(), schemaName.lowercase(), entityName.lowercase()))

        init {
            require(this.components.size == 3) { "Entity name $this is malformed. This is a programmer's error!"}
            require(this.components[0] == NAME_COMPONENT_ROOT) { "${this.components.joinToString(".")} is not a valid schema name." }
        }

        /**
         * Returns parent [SchemaName] for this [EntityName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName = SchemaName(this.components[0], this.components[1])

        /**
         * Generates an [IndexName] as child of this [EntityName].
         *
         * @param name Name of the [IndexName]
         * @return [IndexName]
         */
        fun index(name: String) = IndexName(this.components[0], this.components[1], this.components[2], name)

        /**
         * Generates an [ColumnName] as child of this [EntityName].
         *
         * @param name Name of the [ColumnName]
         * @return [ColumnName]
         */
        fun column(name: String) = ColumnName(this.components[0], this.components[1], this.components[2], name)

        /**
         * Generates the special '__tid' [SequenceName] as child of this [EntityName].
         *
         * @return [SequenceName]
         */
        fun tid() = SequenceName(this.components + "__tid")

        /**
         * Generates an [SequenceName] as child of this [EntityName].
         *
         * @param name Name of the [SequenceName]
         * @return [SequenceName]
         */
        fun sequence(name: String): SequenceName {
            require(name != "__tid") { "The name '__tid' is reserved and cannot be used as sequence name!" }
            return SequenceName(this.components + name)
        }

        /**
         * Checks for a match with another name. Only exact matches, i.e. equality, are possible for [EntityName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)

        override fun equals(other: Any?): Boolean = other is EntityName && other.components.contentEquals(this.components)
        override fun hashCode(): Int = 42 + this.components.contentHashCode()
        override fun toString(): String = this.components.joinToString(NAME_COMPONENT_DELIMITER)
    }

    /**
     * A [Name] object used to identify an index in Cottontail DB
     */
    data class IndexName(override val components: Array<String>) : Name() {

        constructor(schemaName: String, entityName: String, indexName: String): this(arrayOf(NAME_COMPONENT_ROOT, schemaName.lowercase(), entityName.lowercase(), indexName.lowercase()))
        constructor(root: String, schemaName: String, entityName: String, indexName: String): this(arrayOf(root.lowercase(), schemaName.lowercase(), entityName.lowercase(), indexName.lowercase()))

        init {
            require(this.components.size == 4) { "Index name $this is malformed. This is a programmer's error!"}
            require(this.components[0] == NAME_COMPONENT_ROOT) { "${this.components.joinToString(".")} is not a valid index name." }
        }

        /**
         * Returns parent [SchemaName] of this [IndexName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName = SchemaName(this.components[0], this.components[1])

        /**
         * Returns parent  [EntityName] of this [IndexName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName = EntityName(this.components[0], this.components[1], this.components[2])

        /**
         * Checks for a match with another name. Only exact matches, i.e. equality, are possible for [IndexName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)

        override fun equals(other: Any?): Boolean = other is IndexName && other.components.contentEquals(this.components)
        override fun hashCode(): Int = 42 + this.components.contentHashCode()
        override fun toString(): String = this.components.joinToString(NAME_COMPONENT_DELIMITER)
    }

    /**
     * A [Name] object used to identify an column in Cottontail DB
     */
    data class ColumnName(override val components: Array<String>): Name() {

        constructor(columnName: String): this(arrayOf(NAME_COMPONENT_ROOT, NAME_COMPONENT_WILDCARD, NAME_COMPONENT_WILDCARD, columnName.lowercase()))
        constructor(entityName: String, columnName: String): this(arrayOf(NAME_COMPONENT_ROOT, NAME_COMPONENT_WILDCARD, entityName.lowercase(), columnName.lowercase()))
        constructor(schemaName: String, entityName: String, columnName: String): this(arrayOf(NAME_COMPONENT_ROOT, schemaName.lowercase(), entityName.lowercase(), columnName.lowercase()))
        constructor(root: String, schemaName: String, entityName: String, columnName: String): this(arrayOf(root.lowercase(), schemaName.lowercase(), entityName.lowercase(), columnName.lowercase()))

        init {
            require(this.components.size == 4) { "Column name $this is malformed. This is a programmer's error!"}
            require(this.components[0] == NAME_COMPONENT_ROOT) { "${this.components.joinToString(".")} is not a valid column name." }
        }

        /** True if this [ColumnName] is a wildcard. */
        val wildcard: Boolean
            get() = this.components.any { it == NAME_COMPONENT_WILDCARD }

        /**
         * Returns parent [SchemaName] of this [ColumnName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName? {
            if (this.components[1] == NAME_COMPONENT_WILDCARD) return null
            return SchemaName(this.components[0], this.components[1])
        }

        /**
         * Returns parent [EntityName] of this [ColumnName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName? {
            if (this.components[1] == NAME_COMPONENT_WILDCARD || this.components[2] == NAME_COMPONENT_WILDCARD) return null
            return EntityName(NAME_COMPONENT_ROOT, this.components[1], this.components[2])
        }

        /**
         * Checks for a match with another name. Wildcard matches  are possible for [ColumnName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean {
            if (other !is ColumnName) return false
            if (!this.wildcard) return (this == other)
            for ((i, c) in this.components.withIndex()) {
                if (c != NAME_COMPONENT_WILDCARD && c != other.components[i]) {
                    return false
                }
            }
            return true
        }

        override fun equals(other: Any?): Boolean = other is ColumnName && other.components.contentEquals(this.components)
        override fun hashCode(): Int = 42 + this.components.contentHashCode()
        override fun toString(): String = if (this.components[1] == NAME_COMPONENT_WILDCARD && this.components[2] == NAME_COMPONENT_WILDCARD) {
            components[3]
        } else {
            this.components.joinToString(NAME_COMPONENT_DELIMITER)
        }
    }
}