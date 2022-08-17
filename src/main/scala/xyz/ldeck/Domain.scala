import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.concurrent.duration._
import scala.jdk.javaapi.DurationConverters._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Domain {
  type UserId = String
  type Profile = String
  type Product = String
  type OrderId = String

  case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)
  case class Discount(profile: Profile, amount: Double)
  case class Payment(orderId: OrderId, status: String)
}

object CustomImplicits {
  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes
    val deserializer = (aAsBytes: Array[Byte]) => {
      val aAsString = new String(aAsBytes)
      val aOrError = decode[A](aAsString)
      aOrError match {
        case Right(a) => Option(a)
        case Left(error) =>
          println(s"There was an error converting the message $aOrError, $error")
          Option.empty
      }
    }
    Serdes.fromFn[A](serializer, deserializer)
  }
}

object Topics {
  final val OrdersByUserTopic = "orders-by-user"
  final val OrdersTopic = "orders"

  final val LatestDiscountsTopic = "discounts"
  final val LatestDiscountProfilesByUserTopic = "discount-profiles-by-user"

  final val PaymentsTopic = "payments"
  final val PaidOrdersTopic = "paid-orders"
}

case class CoreStreams(builder: StreamsBuilder) {
  import Domain._
  import Topics._
  import CustomImplicits._

  val usersOrdersStreams: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUserTopic)

  val discountProfileGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](LatestDiscountsTopic)
  val discountProfilesByUserTable: KTable[UserId, Profile] = builder.table[UserId, Profile](LatestDiscountProfilesByUserTopic)

  val paymentsStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](PaymentsTopic)
}

case class PaidOrdersTopologyBuilder(timeWindows: TimeWindows) {
  def build(cs: CoreStreams): Topology = {
    import Domain._
    import Topics._
    import CustomImplicits._

    //val expensiveOrders: KStream[UserId, Order] = cs.usersOrdersStreams.filter { (orderId, order) => order.amount >= 1000 }
    //expensiveOrders.to("suspicious-orders")

    //val purchasedListOfProductsStream: KStream[UserId, List[Product]] = cs.usersOrdersStreams.mapValues { order => order.products }
    //val purchasedProductsStream: KStream[UserId, Product] = cs.usersOrdersStreams.flatMapValues { order => order.products }
    //purchasedProductsStream.foreach { (userId, product) => println(s"The user $userId purchased the product $product") }

    //val productsPurchasedByUsers: KGroupedStream[UserId, Product] = purchasedProductsStream.groupByKey
    //val productsPurchasedByUsersFirstInitial: KGroupedStream[String, Product] = purchasedProductsStream.groupBy[String] { (userId, products) => userId.substring(0, 1).toLowerCase }
    //val countOfProductsByUser: KTable[UserId, Long] = productsPurchasedByUsers.count()

    // val countOfProductsByUserEveryTenSeconds: KTable[Windowed[UserId], Long] =
    //   productsPurchasedByUsers
    //     .windowedBy(timeWindows)
    //     .aggregate[Long](0L) { (userId, product, counter) => counter + 1 }


    val ordersWithUserProfileStream: KStream[UserId, (Order, Profile)] = cs.usersOrdersStreams.join[Profile, (Order, Profile)](cs.discountProfilesByUserTable) { (order, profile) => (order, profile) }

    val discountedOrdersStream: KStream[UserId, Order] = ordersWithUserProfileStream.join[Profile, Discount, Order](cs.discountProfileGTable)(
      { case (_, (_, profile)) => profile }, // joining key
      { case ((order, _), discount) => order.copy(amount = order.amount * discount.amount) }
    )

    val ordersStream: KStream[OrderId, Order] = discountedOrdersStream.selectKey { (_, order) => order.orderId }

    val paidOrders: KStream[OrderId, Order] = {
      val joinOrdersAndPayments = (order: Order, payment: Payment) =>
        if (payment.status == "PAID") Option(order) else Option.empty[Order]

      val joinWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.of(5, ChronoUnit.MINUTES))

      ordersStream.join[Payment, Option[Order]](cs.paymentsStream)(joinOrdersAndPayments, joinWindow)
        .flatMapValues(maybeOrder => maybeOrder.iterator.to(Iterable))
    }

    paidOrders.to(PaidOrdersTopic)

    cs.builder.build()
  }
}

object KafkaStreams101App extends App {
  import Domain._

  val windowOfTenSeconds: TimeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10))

  val cs = CoreStreams(new StreamsBuilder)
  val topology: Topology = PaidOrdersTopologyBuilder(windowOfTenSeconds).build(cs)
  println(topology.describe())


  val producers = Future[String] {
    Thread.sleep(5000)
    val props = new Properties
    props.put("bootstrap.servers", "localhost:29092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)
    producer.send(new ProducerRecord[String, String](Topics.LatestDiscountsTopic, "profile1", Discount(profile = "profile1", amount = 0.5D).asJson.noSpaces.toString()))
    producer.send(new ProducerRecord[String, String](Topics.LatestDiscountsTopic, "profile2", Discount(profile = "profile2", amount = 0.25D).asJson.noSpaces.toString()))
    producer.send(new ProducerRecord[String, String](Topics.LatestDiscountsTopic, "profile3", Discount(profile = "profile3", amount = 0.15D).asJson.noSpaces.toString()))
    println("produced discounts")

    producer.send(new ProducerRecord[String, String](Topics.LatestDiscountProfilesByUserTopic, "Daniel", "profile1"))
    producer.send(new ProducerRecord[String, String](Topics.LatestDiscountProfilesByUserTopic, "Riccardo", "profile2"))
    println("produced discounts by user")

    producer.send(new ProducerRecord[String, String](Topics.OrdersByUserTopic, "Daniel", Order(orderId = "order1", user = "Daniel", products = List("iPhone 13", "MacBook Pro 15"), amount = 4000.0).asJson.noSpaces.toString()))
    producer.send(new ProducerRecord[String, String](Topics.OrdersByUserTopic, "Riccardo", Order(orderId = "order2", user = "Riccardo", products = List("iPhone 11"), amount = 800.0).asJson.noSpaces.toString()))
    println("produced orders by user")

    producer.send(new ProducerRecord[String, String](Topics.PaymentsTopic, "order1", Payment(orderId = "order1", status = "PAID").asJson.noSpaces.toString()))
    producer.send(new ProducerRecord[String, String](Topics.PaymentsTopic, "order2", Payment(orderId = "order2", status = "PENDING").asJson.noSpaces.toString()))
    println("produced payments")

    "Produced"
  }

  val props = new Properties
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass())
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass())

  val application: KafkaStreams = new KafkaStreams(topology, props)
  println("starting...")
  application.start() // will block for global tables

}
