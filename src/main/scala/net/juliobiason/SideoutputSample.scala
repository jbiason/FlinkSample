package net.juliobiason

// Yes, you can import multiple elements from a module in Scala;
// I just prefer to use each on a single line to a) see how huge
// the import list is getting, which is usually a point that I should
// move things to another module and b) it makes really easy to
// sort the list in VIM.
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Basic trait (interface) for all metrics.
 *
 * Also notice that all metrics have static values; they are designed
 * this way to be immutable, so all functions that change any value should
 * actually return a new instance of the metric.
 */
trait Metric {
  val prefix:String
  val name:String
  val eventTime:Long
  def add(another:Metric):Metric
  def updateTime(newEventTime:Long):Metric

  def key():String = {
    prefix + "-" + name
  }
}

/**
 * The simple metric have a single value and must be saved in a sink
 * that accepts a single value.
 */
case class SimpleMetric(override val name:String, override val eventTime:Long, value:Int) extends Metric {
  override val prefix = "simple"

  override def add(another:Metric):Metric = {
    val other:SimpleMetric = another.asInstanceOf[SimpleMetric]
    println(s"Adding ${other} into ${this}")
    new SimpleMetric(name, eventTime, value + other.value)
  }

  override def updateTime(newEventTime:Long):Metric = {
    println(s"Updating ${this} to have event time at ${newEventTime}")
    new SimpleMetric(name, newEventTime, value)
  }

  override def toString():String = {
    s"Simple Metric of ${name} [${eventTime}] with value ${value}"
  }
}

/**
 * The complex metric have more than one value and, thus, should be saved
 * in a different sink than the `SimpleMetric`
 */
case class ComplexMetric(override val name:String, override val eventTime:Long, value1:Int, value2:Int, value3:Int) extends Metric {
  override val prefix = "complex"

  override def add(another:Metric):Metric = {
    val other:ComplexMetric = another.asInstanceOf[ComplexMetric]
    println(s"Adding ${other} into ${this}")
    new ComplexMetric(
      name,
      eventTime,
      value1 + other.value1,
      value2 + other.value2,
      value3 + other.value3)
  }

  override def updateTime(newEventTime:Long):Metric = {
    println(s"Updating ${this} to have event time at ${newEventTime}")
    new ComplexMetric(name, newEventTime, value1, value2, value3)
  }

  override def toString():String = {
    s"Complex Metric of ${name} [${eventTime}] with values ${value1}, ${value2}, ${value3}"
  }
}

object SideouputSample {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)   // just random, we usually run with a higher parallelism

    // side output names
    val outputSimple = OutputTag[Metric]("simple")
    val outputComplex = OutputTag[Metric]("complex")
    val outputLate = OutputTag[Metric]("late")

    // Source would be, usually, Kafka, but for this example, we'll use a function
    // that generates a fixed set of elements.
    val source = env.addSource(new SourceFunction[String]() {
      def run(ctx:SourceFunction.SourceContext[String]) {
        val data = List(
          "2018-04-03T14:20:00+00:00\tevent1\t1\t2\t3",
          "2018-04-03T14:20:10+00:00\tevent2\t1\t2\t3",
          "2018-04-03T14:20:20+00:00\tevent1\t1\t2\t3",
          "2018-04-03T14:21:00+00:00\tevent1\t1\t2\t3",
          "2018-04-03T14:21:00+00:00\tevent2\t1\t2\t3",
          "2018-04-03T14:21:00+00:00\tevent1\t1\t2\t3",
          "2018-04-03T14:22:00+00:00\tevent2\t1\t2\t3",
          "2018-04-03T14:22:00+00:00\tevent2\t1\t2\t3")
        for (record <- data) {
          println(s"Adding ${record} to be processed in the pipeline...")
          ctx.collect(record)
        }
      }

      def cancel() {}
    })

    val pipeline = source
      // convert lines to maps, to make them easier to extract data
      // (in reality, our "data" is a bunch of records, so we explode
      //  the data here)
      .flatMap(new FlatMapFunction[String, Map[String, String]] {
        val fieldNames = List(
          "time",
          "eventName",
          "importantValue",
          "notSoImportantValue",
          "reallyNotImportantValue")
        override def flatMap(input:String, output:Collector[Map[String, String]]):Unit = {
          val result = fieldNames.zip(input.split("\t")).toMap
          println(s"Mapped event ${result}...")
          output.collect(result)
        }
      })
      // from each line/map, create the necessary metrics (in this case, 2 metrics for each
      // line)
      .flatMap(new FlatMapFunction[Map[String, String], Metric] {
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

        override def flatMap(input:Map[String, String], output:Collector[Metric]):Unit = {
          val time = format.parse(input("time").asInstanceOf[String]).getTime

          val simpleMetric = new SimpleMetric(
            input("eventName").asInstanceOf[String],
            time,
            input("importantValue").toInt)
          println(s"Created ${simpleMetric}...")
          val complexMetric = new ComplexMetric(
            input("eventName").asInstanceOf[String],
            time,
            input("importantValue").toInt,
            input("notSoImportantValue").toInt,
            input("reallyNotImportantValue").toInt)
          println(s"Created ${complexMetric}...")
          output.collect(simpleMetric)
          output.collect(complexMetric)
        }
      })
      // window assignment
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Metric] {
        var currentMaxTimestamp: Long = 0

        override def extractTimestamp(element:Metric, previousElementTimestamp:Long): Long = {
          val eventTime = element.eventTime
          currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTime)
          eventTime
        }

        override def getCurrentWatermark():Watermark = {
          new Watermark(currentMaxTimestamp - 1000)   // ms, so 1 second lag before firing
        }
      })
      // group things in windows
      .keyBy(_.key)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .allowedLateness(Time.seconds(30))
      .sideOutputLateData(outputLate)
      // "compact" all the entries, by merging them together
      .reduce(
        new ReduceFunction[Metric] {
          override def reduce(elem1:Metric, elem2:Metric):Metric = {
            println(s"Reducing ${elem1} with ${elem2}")
            elem1.add(elem2)
          }
        },
        // because this happens when the window closes, the start of
        // the event is, actually, the time the window opened
        // (this works because we're using Tumbling windows)
        new WindowFunction[Metric, Metric, String, TimeWindow] {
          def apply(
            key:String,
            window:TimeWindow,
            elements:Iterable[Metric],
            out:Collector[Metric]
          ):Unit = {
            println(s"Grouping ${elements.toList.length} elements at ${window.getStart}")

            for (record <- elements) {
              val updatedEvent = record.updateTime(window.getStart)
              out.collect(updatedEvent)
            }
          }
        }
      )

    // Split each metric (based on their classes) on a different sideout, which
    // we'll plug a different sink.
    val result = pipeline
      .process(new ProcessFunction[Metric, Metric] {
        override def processElement(
          value:Metric,
          ctx:ProcessFunction[Metric, Metric]#Context,
          out:Collector[Metric]
        ):Unit = {
          value match {
            case record:SimpleMetric => {
              println(s"Sending ${record} to ${outputSimple}")
              ctx.output(outputSimple, record)
            }
            case record:ComplexMetric => {
              println(s"Sending ${record} to ${outputComplex}")
              ctx.output(outputComplex, record)
            }
            case record => println(s"Don't know how to handle ${record}")
          }
        }
      })

    // collect all simple metrics
    result
      .getSideOutput(outputSimple)
      // the sink would, usually, be the JDBCOutputFormat here, but we
      // only want to print the results, so this will do, pig.
      .addSink(new SinkFunction[Metric] {
        def invoke(value:Metric):Unit = {
          println(s"Got ${value} in the simple output")
        }
      })

    result
      .getSideOutput(outputComplex)
      .addSink(new SinkFunction[Metric] {
        def inkoke(value:Metric):Unit = {
          println(s"Got ${value} in the complex output")
        }
      })

    // execute program
    env.execute("Sample sideoutput")
  }
}
