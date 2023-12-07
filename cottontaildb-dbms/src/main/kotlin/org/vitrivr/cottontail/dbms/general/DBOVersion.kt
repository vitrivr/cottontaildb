package org.vitrivr.cottontail.dbms.general

/**
 * Enum listing the different [DBOVersion]s for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class DBOVersion {
    /** Undefined [DBOVersion]. Used as placeholder in certain areas. */
    UNDEFINED,

    /** The first, legacy version of the Cottontail DB file organisation. */
    V1_0,

    /** The second, iteration of the Cottontail DB file organisation which was introduced in preparation of the HARE column format. */
    V2_0,

    /** The third, iteration of the Cottontail DB file organisation, which is based on JetBrains Xodus. */
    V3_0,

    /** The fourth, iteration of the Cottontail DB file organisation, which is based on JetBrains Xodus. */
    V3_1;

    companion object {
        fun current() = DBOVersion.V3_1
    }
}