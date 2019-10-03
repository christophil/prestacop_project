/*import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {

    val filename = "src/test.txt"
    for (line <- Source.fromFile(filename).getLines) {
      println(line)
    }

  }
}*/

// ----

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
object KafkaProducerApp extends App {

  val props:Properties = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")
  val producer = new KafkaProducer[String, String](props)
  val topic = "test"
  try {
    for (i <- 0 to 15) {
      val record = new ProducerRecord[String, String](topic, i.toString, "My Site is sparkbyexamples.com " + i)
      val metadata = producer.send(record)
      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(),
        metadata.get().partition(),
        metadata.get().offset())
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    producer.close()
  }
}