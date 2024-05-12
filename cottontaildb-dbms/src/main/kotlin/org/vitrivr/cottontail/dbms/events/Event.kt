package org.vitrivr.cottontail.dbms.events

/**
 * An [Event] used in Cottontail DB's internal signalling & logging infrastructure.
 *
 * [Event]s can be used to signal relevant changes to the system, schema or data within and across transactions.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface Event