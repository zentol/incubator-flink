{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

DataStream<String> lines = env.addSource(...);
// build your program

env.execute();
{% endhighlight %}