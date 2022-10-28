package system

import ClientConfig
import io.javalin.http.Context
import org.vitrivr.cottontail.client.iterators.Tuple

object SystemController {

    @Suppress("unused")
    class TxInfo(result: Tuple) {
        val txId: Long? = result.asLong(0)
        val type: String? = result.asString(1)
        val state: String? = result.asString(2)
        val lockCount = result.asInt(3)
        val txCount: Int? = result.asInt(4)
        val created: String = result.asDate(5).toString()
        val ended: String = result.asDate(6).toString()
        val duration: Double? = result.asDouble(7)
    }

    @Suppress("unused")
    class LocksInfo(result: Tuple){
        val dbo: String? = result.asString(0)
        val mode: String? = result.asString(1)
        val ownerCount: Int? = result.asInt(2)
        val owners: String? = result.asString(3)
    }


    fun listTransactions(context: Context) {
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        val result = clientConfig.client.transactions()
        val txInfoArray : MutableList<TxInfo> = mutableListOf()
        result.forEach {
            if (it.asString("state") == "RUNNING" || it.asString("state") == "ERROR") {
                txInfoArray.add(TxInfo(it))
            }
        }
        println(txInfoArray[0].lockCount?.javaClass?.name)
        context.json(txInfoArray)
    }

    fun killTransaction(context: Context) {
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        clientConfig.client.kill(context.pathParam("txId").toLong())
    }

    fun listLocks(context: Context) {
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        val result = clientConfig.client.locks()
        val locksInfoArray = mutableListOf<LocksInfo>()
        result.forEach {
            locksInfoArray.add(LocksInfo(it))
        }
        context.json(locksInfoArray)
    }
}