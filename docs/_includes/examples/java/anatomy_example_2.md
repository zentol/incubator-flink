{% highlight java %}
DataStream<String> input = ...

DataStream<Integer> parsed = input.map(new MapFunction<String, Integer>() {
	@Override
	public Integer map(String value) {
		return Integer.parseInt(value);
	}
});
{% endhighlight %}