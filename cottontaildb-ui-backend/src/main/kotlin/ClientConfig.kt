import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.client.SimpleClient

object ClientConfig {

    private const val host: String = "localhost"
    private const val port: Int = 1865

    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
        .enableFullStreamDecompression()
        .usePlaintext()
        .build()

    val client = SimpleClient(channel)

}