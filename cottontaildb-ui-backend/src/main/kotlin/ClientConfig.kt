import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.client.SimpleClient

class ClientConfig (port : Int) {

    private val host: String = "localhost"


    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
        .enableFullStreamDecompression()
        .usePlaintext()
        .build()

    val client = SimpleClient(channel)

}