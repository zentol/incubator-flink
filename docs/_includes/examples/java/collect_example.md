{% highlight java %}
import org.apache.flink.streaming.api.datastream.DataStreamUtils;

DataStream<Tuple2<String, Integer>> myResult = ...
Iterator<Tuple2<String, Integer>> myOutput = DataStreamUtils.collect(myResult);
{% endhighlight %}