package com.mwrobel.kafkastreams.example_commands

import com.mwrobel.kafkastreams.example_commands.models.Commands.RemoveContactRequest
import org.scalatest.{FunSuite, Matchers}

class SpecialSerdeConvertTest extends FunSuite with Matchers {
  test("extract") {

    val expected = RemoveContactRequest(
      userId = "x",
      causationEventName = "foo",
      causationId = "biz",
      lateArrivingId = None
    )
    val jsonString = RemoveContactRequest.serde.serializer().serialize("", expected)

    val expectedFoo = SpecialDeserializer.deserialiseToTheRightType(jsonString)

    expectedFoo.get.shouldEqual(expected)
  }

}
