import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{Attributes, FlowShape, Inlet, KillSwitches, Materializer, Outlet, OverflowStrategy, UniqueKillSwitch}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.concurrent.{Await, Future, duration}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.language.postfixOps

object AkkaStreamExample extends App {
  val system = ActorSystem("MaterializingStreams")
  implicit val materializer: Materializer = Materializer(system)

  val simpleSource = Source(1 to 10)
  val simplePrintFlow = Flow[Int].map{x =>
    println(s"flow: $x")
    x
  }
  val simpleFlow = Flow[Int].map(x => x + 1)
  val simpleSink = Sink.foreach[Int](println)
  val simpleSinkGroup = Sink.foreach[Iterable[Int]](println)
  val simpleSinkAlso = Sink.foreach[Int](x => println(s"also sink: $x"))

  println("----------------- simple graph ------------------")
  val simpleGraph = simpleSource
    .via(simpleFlow)
    .alsoTo(simpleSink)
    .to(simpleSink)

  simpleGraph.run()(materializer)

//  println("----------------- slow graph ------------------")
//  val slowGraph = simpleSource.
//    via(simplePrintFlow)
//    .buffer(1, OverflowStrategy.backpressure)
//    .throttle(1, FiniteDuration(1, duration.SECONDS))
//    .via(simpleFlow)
//    .to(simpleSink)
//
//  slowGraph.run()

  println("----------------- kill switch ------------------")
  val graph: RunnableGraph[(UniqueKillSwitch, Future[Done])] = simpleSource
    .viaMat(KillSwitches.single)(Keep.right) // Mat = UniqueKillSwitch
//    .throttle(1, 1 seconds) // Keep.left  UniqueKillSwitch
//    .via(new CustomGraph())
    .grouped(2)
    .mapAsync(5){el =>
      Future {
        println(s"run in async: $el")
        Thread.sleep(500)
        el
      }(system.dispatcher)
    }

//    .via(simpleFlow) // Keep.left  UniqueKillSwitch
    .toMat(simpleSinkGroup)(Keep.both)  // (UniqueKillSwitch, Future)

  val (killSwitcher, future) = graph.run()
  Thread.sleep(2000)
//  killSwitcher.shutdown()
  println("--- STOP ---")

//  Await.ready(result, Duration.Inf)

  Thread.sleep(10000)
  println("----------------- END ------------------\n\n")
  system.terminate()
//  future.onComplete { _ =>
//    system.terminate()
//    println("----------------- END ------------------\n\n")
//  }(system.dispatcher)
}


class CustomGraph() extends GraphStage[FlowShape[Int, Int]] {

  val in: Inlet[Int] = Inlet("InputStreamEntries")
  val out: Outlet[Int] = Outlet("AggregatedStreamEntry")
  override val shape: FlowShape[Int, Int] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var count: Int = 0

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    })

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        grab(in)
        count += 1
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        emit(out, count)
        complete(out)
      }
    })
  }
}
