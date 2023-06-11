package org.vitrivr.cottontail.core.database

import kotlinx.serialization.Serializable

/**
 * A [Name] that identifies a DBO used within Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed interface Name: Comparable<Name> {

    companion object {
        /* Delimiter between name components. */
        const val DELIMITER = "."

        /* Character used for wildcards. */
        const val WILDCARD = "*"

        /* Root name component in Cottontail DB names. */
        const val ROOT = "warren"
    }

    /** Returns the [RootName] reference. */
    val root: RootName
        get() = RootName

    /** Returns the last component of this [Name], i.e. the simple name. */
    val fqn: String

    /** Returns the last component of this [Name], i.e. the simple name. */
    val simple: String

    /** Returns true if this [Name] matches the other [Name]. */
    fun matches(other: Name): Boolean

    /**
     * Compares this [Name] to the other [Name] by comparing their fully qualified name.
     *
     * @param other The other [Name] to compare this [Name] to.
     * @return -1, 0 or 1 If the other name is smaller, equal or greater than this name.
     */
    override fun compareTo(other: Name): Int = this.fqn.compareTo(other.fqn)

    /**
     * The [RootName] which always is 'warren'.
     */
    object RootName : Name {
        override val fqn: String = ROOT
        override val simple: String = ROOT
        fun schema(name: String) = SchemaName(name)
        override fun toString(): String = ROOT
        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a function in Cottontail DB.
     */
    @Serializable
    data class FunctionName(val functionName: String): Name {

        companion object {
            /**
             * Parses a [String] to a [FunctionName].
             *
             * @return [FunctionName]
             */
            fun parse(string: String): FunctionName {
                val split = string.split(DELIMITER)
                return when(split.size) {
                    1 -> FunctionName(split[0])
                    2 -> {
                        require(split[0].lowercase() == ROOT) { "Invalid column name: $string" }
                        FunctionName(split[1])
                    }
                    else -> throw IllegalArgumentException("Invalid column name: $string")
                }
            }
        }

        /** The fully qualified name of this [Name.FunctionName]. */
        override val fqn: String
            get() = "${ROOT}${DELIMITER}${this.functionName}"

        /** The simple name of this [Name.FunctionName]. */
        override val simple: String
            get() = this.functionName

        /**
         * Checks for a match with another [Name]. Only exact matches, i.e. equality, are possible for [SchemaName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)

        override fun toString(): String = this.fqn
    }

    /**
     * A [Name] object used to identify a schema in Cottontail DB.
     */
    @Serializable
    data class SchemaName(val schemaName: String): Name {


        companion object {
            /**
             * Parses a [String] to a [SchemaName].
             *
             * @return [SchemaName]
             */
            fun parse(string: String): SchemaName {
                val split = string.split(DELIMITER)
                return when(split.size) {
                    1 -> SchemaName(split[0])
                    2 -> {
                        require(split[0].lowercase() == ROOT) { "Invalid column name: $string" }
                        SchemaName(split[1])
                    }
                    else -> throw IllegalArgumentException("Invalid column name: $string")
                }
            }
        }

        init {
            require(!this.schemaName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.schemaName.contains(WILDCARD)) { "Name component cannot contain ${WILDCARD}."}
        }

        /** The fully qualified name of this [Name.SchemaName]. */
        override val fqn: String
            get() = "${ROOT}${DELIMITER}${this.schemaName}"

        /** The simple name of this [Name.SchemaName]. */
        override val simple: String
            get() = this.schemaName

        /**
         * Generates an [EntityName] as child of this [SchemaName].
         *
         * @param name Name of the [EntityName]
         * @return [EntityName]
         */
        fun entity(name: String) = EntityName(this.schemaName, name)

        /**
         * Checks for a match with another [Name]. Only exact matches, i.e. equality, are possible for [SchemaName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)

        override fun toString(): String = this.fqn
    }

    /**
     * A [Name] object used to identify an entity in Cottontail DB.
     */
    @Serializable
    data class EntityName(val schemaName: String, val entityName: String): Name {

        companion object {
            /**
             * Parses a [String] to a [EntityName].
             *
             * @return [EntityName]
             */
            fun parse(string: String): EntityName {
                val split = string.split(DELIMITER)
                return when(split.size) {
                    2 -> EntityName(split[0], split[1])
                    3 -> {
                        require(split[0].lowercase() == ROOT) { "Invalid column name: $string" }
                        EntityName(split[1], split[2])
                    }
                    else -> throw IllegalArgumentException("Invalid column name: $string")
                }
            }
        }


        init {
            require(!this.schemaName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.schemaName.contains(WILDCARD)) { "Name component cannot contain ${WILDCARD}."}
            require(!this.entityName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.entityName.contains(WILDCARD)) { "Name component cannot contain ${WILDCARD}."}
        }

        /** The fully qualified name of this [Name.EntityName]. */
        override val fqn: String
            get() = "${ROOT}${DELIMITER}${this.schemaName}${DELIMITER}${this.entityName}"

        /** The fully qualified name of this [Name.EntityName]. */
        override val simple: String
            get() = this.entityName

        /**
         * Returns parent [SchemaName] for this [EntityName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName = SchemaName(this.schemaName)

        /**
         * Generates an [IndexName] as child of this [EntityName].
         *
         * @param name Name of the [IndexName]
         * @return [IndexName]
         */
        fun index(name: String) = IndexName(this.schemaName, this.entityName, name)

        /**
         * Generates an [ColumnName] as child of this [EntityName].
         *
         * @param name Name of the [ColumnName]
         * @return [ColumnName]
         */
        fun column(name: String) = ColumnName(this.schemaName, this.entityName, name)

        /**
         * Generates the special '__tid' [SequenceName] as child of this [EntityName].
         *
         * @return [SequenceName]
         */
        fun tid() = SequenceName(this.schemaName, this.entityName, "__tid")

        /**
         * Generates an [SequenceName] as child of this [EntityName].
         *
         * @param name Name of the [SequenceName]
         * @return [SequenceName]
         */
        fun sequence(name: String): SequenceName {
            require(name != "__tid") { "The name '__tid' is reserved and cannot be used as sequence name!" }
            return SequenceName(this.schemaName, this.entityName, name)
        }

        /**
         * Checks for a match with another name. Only exact matches, i.e. equality, are possible for [EntityName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)

        override fun toString(): String = this.fqn
    }

    /**
     * A [Name] object used to identify a sequence.
     */
    @Serializable
    data class SequenceName(val schemaName: String, val entityName: String, val sequenceName: String) : Name {

        init {
            require(!this.schemaName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.schemaName.contains(WILDCARD)) { "Name component cannot contain ${WILDCARD}."}
            require(!this.entityName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.entityName.contains(WILDCARD)) { "Name component cannot contain ${WILDCARD}."}
            require(!this.sequenceName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.sequenceName.contains(WILDCARD)) { "Name component cannot contain ${WILDCARD}."}
        }

        /** The fully qualified name of this [Name.SequenceName]. */
        override val fqn: String
            get() = "${ROOT}${DELIMITER}${this.schemaName}${DELIMITER}${this.entityName}${DELIMITER}${this.sequenceName}"

        /** The fully qualified name of this [Name.SequenceName]. */
        override val simple: String
            get() = this.sequenceName

        /**
         * Returns parent [SchemaName] of this [SequenceName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName = SchemaName(this.schemaName)

        /**
         * Returns parent [EntityName] of this [SequenceName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName = EntityName(this.schemaName, this.entityName)

        /**
         * Checks for a match with another name. Only exact matches, i.e. equality, are possible for [SequenceName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)

        override fun toString(): String = this.fqn
    }

    /**
     * A [Name] object used to identify an index in Cottontail DB
     */
    @Serializable
    data class IndexName(val schemaName: String, val entityName: String, val indexName: String) : Name {

        companion object {
            /**
             * Parses a string to a [IndexName].
             *
             * @return [IndexName]
             */
            fun parse(string: String): IndexName {
                val split = string.split(DELIMITER)
                return when(split.size) {
                    3 -> IndexName(split[0], split[1], split[2])
                    4 -> {
                        require(split[0].lowercase() == ROOT) { "Invalid column name: $string" }
                        IndexName(split[1], split[2], split[3])
                    }
                    else -> throw IllegalArgumentException("Invalid column name: $string")
                }
            }
        }

        init {
            require(!this.schemaName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.schemaName.contains(WILDCARD)) { "Name component cannot contain ${WILDCARD}."}
            require(!this.entityName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.entityName.contains(WILDCARD)) { "Name component cannot contain ${WILDCARD}."}
            require(!this.indexName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.indexName.contains(WILDCARD)) { "Name component cannot contain ${WILDCARD}."}
        }

        /** The fully qualified name of this [Name.IndexName]. */
        override val fqn: String
            get() = "${ROOT}${DELIMITER}${this.schemaName}${DELIMITER}${this.entityName}${DELIMITER}${this.indexName}"

        /** The fully qualified name of this [Name.IndexName]. */
        override val simple: String
            get() = this.indexName

        /**
         * Returns parent [SchemaName] of this [IndexName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName = SchemaName(this.schemaName)

        /**
         * Returns parent  [EntityName] of this [IndexName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName = EntityName(this.schemaName, this.entityName)

        /**
         * Checks for a match with another name. Only exact matches, i.e. equality, are possible for [IndexName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)

        override fun toString(): String = this.fqn
    }

    /**
     * A [Name] object used to identify an column in Cottontail DB
     */
    @Serializable
    data class ColumnName(val schemaName: String, val entityName: String, val columnName: String): Name {

        companion object {

            val ALL_COLUMNS = ColumnName("*", "*", "*")

            /**
             * Parses a string to a [ColumnName].
             *
             * @return [ColumnName]
             */
            fun parse(string: String): ColumnName {
                val split = string.split(DELIMITER)
                return when(split.size) {
                    1 -> ColumnName(WILDCARD, WILDCARD, split[0])
                    2 -> ColumnName(WILDCARD, split[0], split[1])
                    3 -> ColumnName(split[0], split[1], split[2])
                    4 -> {
                        require(split[0].lowercase() == ROOT) { "Invalid column name: $string" }
                        ColumnName(split[1], split[2], split[3])
                    }
                    else -> throw IllegalArgumentException("Invalid column name: $string")
                }
            }
        }

        init {
            require(!this.schemaName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.entityName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
            require(!this.columnName.contains(DELIMITER)) { "Name component cannot contain ${DELIMITER}."}
        }

        /** Constructor for simple names. */
        constructor(columnName: String): this(WILDCARD, WILDCARD, columnName)

        /** The fully qualified name of this [Name.IndexName]. */
        override val fqn: String
            get() = "${ROOT}${DELIMITER}${this.schemaName}${DELIMITER}${this.entityName}${DELIMITER}${this.columnName}"

        /** The fully qualified name of this [Name.IndexName]. */
        override val simple: String
            get() = this.columnName

        /** True if this [ColumnName] is a wildcard. */
        val wildcard: Boolean
            get() = this.schemaName == WILDCARD || this.entityName == WILDCARD || this.columnName == WILDCARD

        /**
         * Returns parent [SchemaName] of this [ColumnName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName? {
            if (this.schemaName == WILDCARD) return null
            return SchemaName(this.schemaName)
        }

        /**
         * Returns parent [EntityName] of this [ColumnName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName? {
            if (this.schemaName == WILDCARD || this.entityName == WILDCARD) return null
            return EntityName(this.schemaName, this.entityName)
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
            if (this.schemaName != WILDCARD && this.schemaName != other.schemaName) return false
            if (this.entityName != WILDCARD && this.entityName != other.entityName) return false
            if (this.columnName != WILDCARD && this.columnName != other.columnName) return false
            return true
        }

        override fun toString(): String = if (this.schemaName == WILDCARD && this.schemaName == WILDCARD) {
            this.simple
        } else {
            this.fqn
        }
    }
}