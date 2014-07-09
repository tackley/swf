import java.util.Date

import com.amazonaws.services.simpleworkflow.model.{WorkflowExecutionStartedEventAttributes, HistoryEvent}
import org.scalatest._

class SwfHistoryEventTest extends FlatSpec with Matchers with OptionValues {
  "SwfHistoryEvent" should "be able to represent a WorkflowExecutionStarted event" in {
    val executionStarted = new HistoryEvent()
      .withEventType("WorkflowExecutionStarted")
      .withEventId(1)
      .withEventTimestamp(new Date(1972, 7, 20, 11, 0))
      .withWorkflowExecutionStartedEventAttributes(
        new WorkflowExecutionStartedEventAttributes()
          .withInput("hello")
      )

    val ev = SwfHistoryEvent.parse(executionStarted)

    println(ev.getClass)
//    ev.input shouldBe "hello"

  }

}
