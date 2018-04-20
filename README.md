# Flink Side Output Sample

This is an example of working with Flink and Side outputs.

## What this is

The pipeline is, basically, processing log lines, turning them into metrics,
reducing the results and applying them to time windows (tumbling windows, in
the Flink jargon, which basically are consecutive blocks of elements split by
their event time). Once the elements are grouped in their windows, we want
them to go to different sinks (stored).

## WARNING!

This code is hard to read on purpose; the general idea was to produce a single
file that could be read in a sitting.

On general, I'd move each Function to its own class and simply add the class
directly into the function call. For example, instead of doing `.process(new
ProcessFunction[Metric, Metric]) { blah, blah blah }`, I'd create a class that
extends `ProcessFunction` and use that class instead in `new ProcessFunction`.

The current way is more explicit, but creating classes directly in the pipeline
makes it a hell to read.

(This is something I'll fix in the future, though.)

## The layout

### Metrics

The metrics are the information being extracted from the log lines. We have
two different metrics: a simple metrics (`SimpleMetric`) that has a single
value and a more complex (`ComplexMetric`) with more values.

The exercise is to have two different types of elements floating in the data
stream, which would require different sinks for each.

Even if those two are different elements, both use the same trait (interface),
so even of both float in the data stream, they are processed in the same way.

### The Source

In this example, we use a function (`SourceFunction`) to generate the
elements. It basically have a list of lines and throw each in the data stream.

### Making it easier to deal with the lines

Because the lines are pure text, we need an easy way to extract the
information on them. For this, we used a `flatMap` to split the lines in their
separator (in this example, a tab character) and then name each field,
creating a map/object/dictionary (Scala and steam/functional processing names
coliding here). This way, when we actually create the metrics, we can simply
request the fields in the map by their names.

Note, though, that each line becomes a single map, so a `map` would also work
here. We simply used `flatMap` because instead of working with a single line
of log, we could work with blocks of lines and the map function would generate
more maps/objects/dictionaries.

### Extracting metrics

To extract the metrics, we use another `flatMap`, this time because we are
extracting more than one metric from each line.

### Windows

As mentioned, we group elements by window, so we need to define how the window
works. The very definition on how we define the time of the events that should
create/close windows is in the very start of the pipeline, when we indicated
to use `TimeCharacteristic.EventTime`, which means "use the time of the event,
instead of the time the log is being processed or some other information".

Because we are using the event time, we need to indicate how the time needs to
be extracted. This is done in
`AssignerWithPeriodicWatermarks.extractTimestamp`. Another thing to notice is
that we also define the watermakr, the point in which, if an even before this
time appears, the window of time it belongs will be fired (sent to the sink).
In this example, it is 1 second after the more recent event that appeared in
the data stream.

The metrics, inside their windows, are grouped by their key, with windows of 1
minute, which will survive for 30 seconds (as this is defined as the late
possible time) and everything sent after this is put on a side output for
later processing.

### Reducing

Once an element is added to a window, it is reduced (in functional jargon)
along elements of its own key. This is what we do with `ReduceFunction` and
the fact that the Metric trait have an `add` method.

### Sinks

When the window fires, we divide the results into two different side outputs
-- remember, we have two different metrics and each require a different sink.
the `ProcessFunction` does that, based on the class of the metric, sending
each metric type to a different side outputs.

Those side outputs are then captured and sent to different sinks.

## Running

Simply, install [SBT](https://www.scala-sbt.org/) and run `sbt run`. SBT will
download all necessary dependencies and run the pipeline in standalone mode.

We didn't test it using the full Jobmanager+TaskManager model of Flink and,
thus, this is given as an exercise to the reader. :)
