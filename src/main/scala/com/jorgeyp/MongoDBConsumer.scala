package com.jorgeyp

import java.util.Properties

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.bson.BsonDocument
import org.mongodb.scala.bson.BsonArray
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{Completed, MongoClient, Observable, Observer}
import wvlet.log.LogSupport

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

// TODO doc and tests
object MongoDBConsumer extends App with LogSupport {

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  // TODO extract to args or config
  val topic="meetup"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "MongoDBConsumer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  private val client = MongoClient(s"mongodb://localhost:27017")
  private val db = client.getDatabase("meetup")
  private val collection = db.getCollection("rsvp")



  val config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("MongoDBConsumer")

  case class Location(x: Float, y: Float)



  val done = Consumer
    .plainSource(consumerSettings, Subscriptions.topics("meetup"))
    .runWith(Sink.foreach {
      case cr: ConsumerRecord[String, String] =>
        info(cr.value())
        val doc: Document = Document(cr.value())
        val group: BsonDocument = doc("group").asDocument()
        val geodoc = doc + ("group_location" -> Document(
          "type" -> "Point",
          "coordinates" -> BsonArray(List(group.get("group_lon"), group.get("group_lat"))),
        ))

        val inserted: Observable[Completed] = collection.insertOne(geodoc)
        inserted.subscribe(new Observer[Completed] {
          override def onNext(result: Completed): Unit = None
          override def onError(e: Throwable): Unit = println("Failed to insert: $geodoc")
          override def onComplete(): Unit = println(s"Inserted: $geodoc")
        })

    })

  info("Starting Kafka => MongoDB consumer")
  done onComplete {
    case Success(status) => info(s"Consumer status: $status"); system.terminate()
    case Failure(err) => error(err.toString); system.terminate()
  }
}
