import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils
import com.amazonaws.services.simpleworkflow.model._
import com.amazonaws.services.simpleworkflow.{AmazonSimpleWorkflowClient, AmazonSimpleWorkflowAsyncClient}
import swf.{ActivityTaskCompleted, WorkflowExecutionStarted, SwfHistoryEvent}

import scala.annotation.tailrec
import scala.collection.convert.wrapAll._
import scala.concurrent.Future

import scala.concurrent.ExecutionContext.Implicits.global


class Decider(swf: AmazonSimpleWorkflowClient, domain: String) {
  @tailrec
  final def run() {
    log("polling...")

    val decisionTask = swf.pollForDecisionTask(
      new PollForDecisionTaskRequest()
        .withDomain(domain)
        .withIdentity("graham's laptop")
        .withTaskList(new TaskList().withName("swarmize"))
    )

    log("done: " + decisionTask)

    if (Option(decisionTask.getTaskToken).isDefined) {

      val events = decisionTask.getEvents.map(SwfHistoryEvent.parse)

      // ignore the events that tell use the decider has been invoked
      val nonDecisionEvents = events.filterNot(_.isDecisionEvent)

      val lastInterestingEvent = nonDecisionEvents.last

      log(s"lastEvent was $lastInterestingEvent")

      val decision = lastInterestingEvent match {
        case e: WorkflowExecutionStarted =>
          new Decision()
            .withDecisionType(DecisionType.ScheduleActivityTask)
            .withScheduleActivityTaskDecisionAttributes(
              new ScheduleActivityTaskDecisionAttributes()
                .withActivityType(new ActivityType().withName("lookup_postcode").withVersion("1.0"))
                .withActivityId("blah")
                .withInput(e.input)
                .withTaskList(new TaskList().withName("swarmize"))
            )

        case e: ActivityTaskCompleted =>
          new Decision()
            .withDecisionType(DecisionType.CompleteWorkflowExecution)
            .withCompleteWorkflowExecutionDecisionAttributes(
              new CompleteWorkflowExecutionDecisionAttributes()
                .withResult(e.result)
            )

        case other =>
          log(s"don't know how to respond to a $other yet")
          new Decision()
            .withDecisionType(DecisionType.FailWorkflowExecution)
            .withFailWorkflowExecutionDecisionAttributes(
              new FailWorkflowExecutionDecisionAttributes()
                .withReason("decider can't respond to event type of " + other)
            )
      }

      log(s"decision = ${WorkflowExecutionUtils.prettyPrintDecision(decision)}" )

      swf.respondDecisionTaskCompleted(
        new RespondDecisionTaskCompletedRequest()
          .withTaskToken(decisionTask.getTaskToken)
          .withDecisions(decision)
      )

    }
    run()
  }

  private def log(msg: String) = println(s"decider: $msg")
}


class Activity(swf: AmazonSimpleWorkflowClient, domain: String) {
  @tailrec
  final def run() {
    log("polling...")

    val activityTask = swf.pollForActivityTask(
      new PollForActivityTaskRequest()
        .withDomain(domain)
        .withIdentity("graham's laptop")
        .withTaskList(new TaskList().withName("swarmize"))
    )

    log("done: " + activityTask)

    if (Option(activityTask.getTaskToken).isDefined) {
      log(s"now should run ${activityTask.getActivityType} with input:\n${activityTask.getInput}")

      swf.respondActivityTaskCompleted(
        new RespondActivityTaskCompletedRequest()
          .withResult("completed processing of " + activityTask.getInput)
          .withTaskToken(activityTask.getTaskToken)
      )

    }

//      swf.respondDecisionTaskCompleted(
//        new RespondDecisionTaskCompletedRequest()
//          .withTaskToken(decisionTask.getTaskToken)
//          .withDecisions(decision)
//      )

    run()
  }

  private def log(msg: String) = println(s"activity: $msg")

}

object Main extends App {
  println("hi")

  val region = Region.getRegion(Regions.US_EAST_1)

  val swf = region.createClient(classOf[AmazonSimpleWorkflowClient], null, null)

  val resp = swf.listDomains(new ListDomainsRequest().withRegistrationStatus(RegistrationStatus.REGISTERED))


  for (info <- resp.getDomainInfos) {
    println(info)
  }


  val myDomain = "swarmize-test"

  Future { new Decider(swf, myDomain).run() }

  new Activity(swf, myDomain).run()

}