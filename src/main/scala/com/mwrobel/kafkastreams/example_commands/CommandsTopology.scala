package com.mwrobel.kafkastreams.example_commands

import com.mwrobel.kafkastreams.example_commands.models.Commands._
import com.mwrobel.kafkastreams.example_commands.models._
import com.mwrobel.kafkastreams.example_commands.state.{ContactDetails, ContactRequest, ContactRequestInitState}
import com.mwrobel.kafkastreams.example_commands.transformers.{AppLogicTransformer, ScheduleContactRequests}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Produced}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.Stores

import java.time.Duration
import java.util.UUID

object Topics {
  val contactDetailsEntity_2p = "contact_details_2p"
  val policyPurchased         = "policy_purchased"
  val quotesCreated_3p        = "quotes_created_3p"
  val internalAppEvents       = "internal_app_commands"
  val dialer                  = "dialer_events"
  val leadScored              = "lead_scored"
  val http_commands           = "http_commands"
}

object CommandsTopology extends LazyLogging {
  import Serdes._

  val appLogicTransformer = new ValueTransformerSupplier[StateChangeCommand, Option[ContactRequest]] {
    override def get(): ValueTransformer[StateChangeCommand, Option[ContactRequest]] =
      new AppLogicTransformer()
  }

  val scheduleTransformer = new TransformerSupplier[String, ContactRequest, KeyValue[String, StateChangeCommand]] {
    override def get(): Transformer[String, ContactRequest, KeyValue[String, StateChangeCommand]] =
      ScheduleContactRequests(scheduleInterval = 5000, setScheduledAt = (contactRequest: ContactRequest) => {
        contactRequest.createdAt.plusSeconds(10)
      })
  }

  def buildTopology()(implicit builder: StreamsBuilder): Unit = {
    setupStores()

    adapters()

    mainAppLogic()
  }

  def mainAppLogic()(implicit builder: StreamsBuilder): Unit = {
    val commandsStream: KStream[String, StateChangeCommand] = builder
      .stream[String, Array[Byte]](Topics.internalAppEvents)
      .flatMapValues(e => SpecialDeserializer.deserialiseToTheRightType(e))

    val updatedStateStream: KStream[String, ContactRequest] = commandsStream
      .transformValues(appLogicTransformer, ContactRequestsStore.name)
      .flatMapValues(v => v) // removes Nones

    val justCreated: KStream[String, ContactRequest] = updatedStateStream
      .filter((_, v) => v.currentState == ContactRequest.created)

    // SCHEDULING
    val scheduleCommands: KStream[String, StateChangeCommand] = justCreated
      .transform(scheduleTransformer, ContactRequestsScheduleStore.name)

    scheduleCommands
      .filter((k, v) => v.isInstanceOf[ScheduleContactRequest])
      .mapValues(v => v.asInstanceOf[ScheduleContactRequest])
      .to(Topics.internalAppEvents)(Produced.`with`(String, ScheduleContactRequest.serde))

    scheduleCommands
      .filter((k, v) => v.isInstanceOf[TriggerContactRequest])
      .mapValues(v => v.asInstanceOf[TriggerContactRequest])
      .to(Topics.internalAppEvents)(Produced.`with`(String, TriggerContactRequest.serde))

    // EXTERNAL
    updatedStateStream
      .filter((k, v) => v.currentState == ContactRequest.triggered)
      .mapValues(v => ContactRequestCreated(userId = v.userId))
      .to(Topics.dialer)(Produced.`with`(String, ContactRequestCreated.serde))

    updatedStateStream
      .filter((k, v) => v.currentState == ContactRequest.prioritized)
      .mapValues(v => ContactRequestPrioritized(userId = v.userId))
      .to(Topics.dialer)(Produced.`with`(String, ContactRequestPrioritized.serde))

    updatedStateStream
      .filter((k, v) => v.currentState == ContactRequest.deleted)
      .mapValues(v => ContactRequestRemoved(userId = v.userId))
      .to(Topics.dialer)(Produced.`with`(String, ContactRequestRemoved.serde))
  }

  def adapters()(implicit builder: StreamsBuilder): Unit = {
    implicit val contactDetailsSerde       = ContactDetailsEntity.serde
    implicit val quotesCreatedSerde        = QuotesCreated.serde
    implicit val contactRequestSerde       = ContactRequest.serde
    implicit val contactRequestInitSerde   = ContactRequestInitState.serde
    implicit val policyPurchasedSerde      = PolicyPurchased.serde
    implicit val leadScoredSerde           = LeadScored.serde
    implicit val removeContactRequestSerde = RemoveContactRequest.serde

    val quotesCreated = builder
      .stream[String, QuotesCreated](Topics.quotesCreated_3p)
      .selectKey((_, v) => v.userId)

    val contactDetails: KTable[String, ContactDetailsEntity] = builder
      .stream[String, ContactDetailsEntity](Topics.contactDetailsEntity_2p)
      .selectKey((_, v) => v.id)
      .groupByKey
      .reduce((_, c2) => c2)

    val policyPurchased = builder
      .stream[String, PolicyPurchased](Topics.policyPurchased)
      .selectKey((_, v) => v.userId)

//    builder
//      .stream[String, LeadScored](Topics.leadScored)
//      .selectKey((_, v) => v.userId)
//      .mapValues(
//        (_, v) =>
//          PrioritizeContactRequest(userId = v.userId, score = 1, causationEventName = "lead_scored", causationId = "1")
//      )
//      .to(Topics.internalAppEvents)(Produced.`with`(String, PrioritizeContactRequest.serde))

//    builder
//      .stream[String, RemoveContactRequest](Topics.http_commands)
//      .selectKey((_, v) => v.userId)
//      .to(Topics.internalAppEvents)(Produced.`with`(String, RemoveContactRequest.serde))

    val contactRequestCreatedStream = quotesCreated
      .leftJoin(contactDetails)(createInitialState)

    // ensure there's no out of order events
    policyPurchased
      .leftJoin(contactRequestCreatedStream)(
        createRemoveCommand,
        JoinWindows.of(Duration.ofMinutes(1))
      )
      .selectKey((_, p) => p.userId)
      .peek((k, v) => logger.info(s"Remove Command, k: ${k}, v: ${v}"))
      .to(Topics.internalAppEvents)(Produced.`with`(String, RemoveContactRequest.serde))

    // all the data, for the initial create
    //    quotesCreated
    //      .leftJoin(contactDetails)(createInitialState)
    contactRequestCreatedStream
      .selectKey((_, cr) => cr.userId)
      .mapValues((_, c) => createContactRequest(c))
      .peek((k, v) => logger.info(s"Create Command: k: ${k}, v: ${v}"))
      .to(Topics.internalAppEvents)(Produced.`with`(String, CreateContactRequest.serde))
  }

  private def createContactRequest(c: ContactRequestInitState) = {
    CreateContactRequest(
      causationEventName = c.causationEventName,
      causationId = c.causationId,
      userId = c.userId,
      contactRequestInitState = c
    )
  }
  private def setupStores()(implicit builder: StreamsBuilder): Unit = {
    val contactRequestStoreSuplier = Stores.persistentKeyValueStore(ContactRequestsStore.name)

    val contactRequestStoreBuilder =
      Stores.keyValueStoreBuilder(
        contactRequestStoreSuplier,
        ContactRequestsStore.keySerde,
        ContactRequestsStore.valSerde
      )

    builder.addStateStore(contactRequestStoreBuilder)

    val scheduleStoreSuplier = Stores.persistentKeyValueStore(ContactRequestsScheduleStore.name)
    val scheduleStoreBuilder =
      Stores.keyValueStoreBuilder(
        scheduleStoreSuplier,
        ContactRequestsScheduleStore.keySerde,
        ContactRequestsScheduleStore.valSerde
      )

    builder.addStateStore(scheduleStoreBuilder)
  }

  def createInitialState(quotesCreated: QuotesCreated, contactDetails: ContactDetailsEntity) = {
    val maybeContactDetails = Option(contactDetails)

    ContactRequestInitState(
      id = UUID.randomUUID.toString(),
      userId = quotesCreated.userId,
      quotesCreated.quotesNumber,
      quotesCreated.reference,
      contactDetails = maybeContactDetails.map(c => ContactDetails(c.name, c.telephoneNumber)),
      causationId = quotesCreated.eventId,
      causationEventName = "quotes_created"
    )
  }

  def createRemoveCommand(
      policyPurchased: PolicyPurchased,
      contactRequest: ContactRequestInitState
  ): RemoveContactRequest = {
    val maybeContactRequest = Option(contactRequest)

    RemoveContactRequest(
      userId = policyPurchased.userId,
      causationEventName = "policy_purchased",
      causationId = policyPurchased.userId,
      lateArrivingId = maybeContactRequest.map(_.id)
    )
  }
}
