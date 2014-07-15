--- 
title: Python Programming Guide
---


Python Programming Guide
=======================

This guide explains how to develop Flink programs with the Python
programming interface. It assumes you are familiar with the general concepts of
Flink's [Programming Model](pmodel.html "Programming Model"). We
recommend to learn about the basic concepts first, before continuing with the
[Java](java.html "Java Programming Guide") or this Python programming guide.

Here we will look at the general structure of a Python job. You will learn how to
write data sources, data sinks, and operators to create data flows that can be
executed using the Flink system.

Writing Python jobs requires an understanding of Python, there is excellent
documentation available [here](https://www.python.org/doc/). Most
of the examples can be understood by someone with a good understanding
of programming in general, though.

<section id="intro-example">
Word Count Example
------------------

To start, let's look at a Word Count job implemented in Python. This program is
very simple but it will give you a basic idea of what a Python job looks like.

```python
from flink.plan.Environment import get_environment
from flink.plan.Constants import Types
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
import re

class Tokenizer(FlatMapFunction):
  def flat_map(self, value, collector):
    words = re.split("\W+", value.lower())
    for word in words:
      collector.collect((1, word))

class Adder(GroupReduceFunction:
  def group_reduce(self, iterator, collector):
    first = iterator.next()
    count = first[0]
    word = first[1]
    
    while iterator.has_next():
      count += iterator.next(){0]
      
    collector.collect((count, word))

if __name__ == "__main__":
  env = get_environment()
  data = env.read_text(textfile)
  
  data \
    .flatmap(Tokenizer(), (Types.INT, Types.STRING)) \
    .group_by(1) \
    .groupreduce(Adder(), (Types.INT, Types.STRING)) \
    .write_csv(outputfile)
  
  env.execute()

```

Same as any Flink job a Python job consists of one or several data
sources, one or several data sinks and operators in between these that transform
data. Together these parts are referred to as the data flow graph. It dictates
the way data is passed when a job is executed.

When using Python in Flink an important concept to grasp is that of the
`DataSet`. `DataSet` is an abstract concept that represents actual data sets at
runtime and which has operations that transform data to create a new transformed
data set. In this example the `env.read_text("/some/input")` call creates a
`DataSet[String]` that represents the lines of text from the input. The
`flatmap` operation is an operation on `DataSet` that passes (at runtime) 
the data items through the provided function to transform them.
The result of the `flatmap` operation is a new `DataSet` that represents the transformed data. 
On this other operations be performed. Another such operation are `group_by` and `groupreduce`, 
but we will go into details of those later in this guide.

The `write_csv` operation of `DataSet` is used to create a data sink. You provide it
with a path where the data is to be written to. You can also write the result as a simple
text file with `write_text` or to the console using `output`.

<section id="intro-example">
Project Setup
-------------

Apart from setting up Flink, no additional work is required. The python package can be found in the
/resource folder of your Flink distribution.

<section id="dataset">
The DataSet Abstraction
-----------------------

As already alluded to in the introductory example you write Python jobs by using
operations on a `DataSet` to create new transformed `DataSet`. This concept is
the core of the Flink Python API so it merits some more explanation. A
`DataSet` can look and behave like a regular Python object in your code but
it does not contain any actual data but only represents data. For example: when
you use `read_text()` you get back a `DataSource[String]` that represents each
line of text in the input as a `String`. No data is actually loaded or available
at this point. The set is only used to apply further operations which themselves
are not executed until the data flow is executed. An operation on `DataSet`
creates a new `DataSet` that represents the transformation and has a pointer to
the `DataSet` that represents the data to be transformed. In this way a tree of
data sets is created that contains both the specification of the flow of data as
well as all the transformations. This graph can be wrapped in a `Plan` and
executed.

Working with the system is like working with lazy collections, where execution
is postponed until the user submits the job.

<section id="datatypes">
Data Types
----------

There are some restrictions regarding the data types that can be used in Python
jobs. Only primitive Python types (bool, int, float, string) aswell as lists or tuples
containing them are currently supported.
Other types will be converted to strings using `str()`.

Several operations require you to explicitly declare types. This is done by passing
an exemplary object that has the desired type. This holds also for tuples.

```python
(Types.INT, Types.STRING)
```
Would denote a tuple containing an int and a string. Note that for Operations that
work strictly on tuples (like grouping), no braces are required.

There are a few Constants defined in `flink.plan.Constants.Types` that allow this
in a more readable fashion.

<section id="data-sources">
Creating Data Sources
---------------------

To get an initial `DataSet` on which to perform operations to build a data flow
graph one of the following methods is used:

```python
input = env.read_text("<file-path>")
input = env.read_csv("<file-path>", <types>)
input = env.from_elements(element1, element2, .. , elementX)
```

The value `input` is now a `DataSet`.
The file path can be on of either `file:///some/file` to acces files on the
local machine or `hdfs://some/path` to read files from HDFS.
We will now have a look at each method and show how they are employed and in
which situations.

<section id="text-input">
#### read_text()

This method simply reads a text file line wise and creates a `String`
for each line. It is used as:
```python
input = env.read_text("<file-path>")
```
The `input` would then be a `DataSet[String]`.

<section id="csv-input">
#### read_csv()

This method is mainly used to read Csv-Files, as the name suggests. Input
files must be text files. They will be read automatically and returned as
Python tuples to you. It is used as:

```python
input = env.read_csv("<file-path>", <types>, line_delimiter, field_delimiter)
```
With `types` you define how the data is read. `line_delimiter` and `field_delimiter` are optional fields.
```python
input = env.read_csv("<file-path>", (Types.INT, Types.STRING))
```

This would return a DataSet that contains pairs of integers and strings.
The default record delimiter is a newline, the default field delimiter is a
comma.

<section id="element-input">
#### from_elements()

This method is used to create a DataSet containing specific elements, ideal for
testing/prototyping. It is used as:

```python
input = env.from_elements(element1, element2, .. , elementX)
```
All elements must be of the same type, and of the same size if they are tuples.

```python
input = env.from_elements((1, "hello"), (2, "world"))
```

<section id="operations">
Operations on DataSet
---------------------

As explained in [Programming Model](pmodel.html#operators),
a Flink job is a graph of operators that process data coming from
sources that is finally written to sinks. When you use the Python front end
these operators as well as the graph is created behind the scenes. For example,
when you write code like this:

```python
data = env.read_text(textfile)

data \
  .flatmap(Tokenizer(), (Types.INT, Types.STRING)) \
  .write_csv(outputfile)
```
What you get is a graph that has a data source, a flatmap operator, and a data sink. You 
do not have to know about this to be able to use the Python front end but
it helps to remember, that when you are using Python you are building
a data flow graph that processes data only when executed.

There are operations on `DataSet` that correspond to all the types of operators
that the Flink system supports. We will shortly go trough all of them with
some examples.

<section id="field-selectors">
#### Field Selectors

For some operations (group, sort, join, and cogroup) it is necessary to specify which
parts of a data type are to be considered the key. This key is used for grouping
elements together for reduce and for joining in case of a join or cogroup.
In Python the key is specified as the index of the tuple field we want to use as the key.
This also means that these operations can only be executed on DataSet's containing tuples.
An example:


```python
data.group_by(0)
```

The int passed to `groupBy` is the key index. In this case we group `data`
on the first field.

#### Map Operation

The `map` Operation is a simple one to one mapping. Take a look at the following
code:

```python

class Double(MapFunction):
  def map(self, value):
    return value*2

 mapped_data = data.map(Double(), Types.INT)
```

This defines a map operator that multiplies every value with 2. So, if the input set 
contained 1, 2 and 3, the result after the operator would be 2, 4 and 6.

#### FlatMap Operation

The `flatMap` operation works a bit differently, here you can return 
multiple values. So for every element in the input data you get a list of results.
The concatenation of those is the result of the operator. If you had
the following code:

```python

class DoubleandOriginal(FlatMapFunction):
  def flat_map(self, value, collector):
    collector.collect(value)
    collector.collect(value * 2)
  
data \
  .flatmap(DoubleAndOriginal(), Types.INT) \
```

and as input 1, 2 and 3, you would receive 1, 2, 3, 2, 4 and 6 as a result.

#### Filter Operation

A `filter`operation evaluates a condition for every element.
The elements for which this condition holds are part of the result of the operation, 
the others are culled. An example for a filter is this:
```python

class Greater(FilterFunction):
  def filter(self, value):
    return value > 5
    
data.filter(Greater())
```
This would retain all values that are greater than 5.

#### Reduce Operation

As explained [here](pmodel.html#operators) Reduce is an operation that looks
at groups of elements at a time and can, for one group, output one or several
elements. To specify how elements should be grouped you need to select grouping
keys, as explained [above](#field-selectors).

Currently there are two variants of reduce operations:

A `reduce` operation combines two elements of one group or DataSet, and repeats
this process until only one element remains. The output type has to be the same
as the input type.

```python
class Adder(ReduceFunction):
  def reduce(self, value1, value2):
    return value1 + value2

data.reduce(Adder())

```

This code would for example add up all numbers in a DataSet.

A `group_reduce` operation on the other hand receives all elements as an iterator
and combines them manually. You can also return multiple results, which can even
have a different type than the input.

```python
class Count(GroupReduceFunction):
  def group_reduce(self, iterator, collector):
    word = iterator.next()[0]
    count = 1
    while iterator.has_next():
      iterator.next()
      count++
    collector.collect(count, word)

data \
  .group_by(0) \
  .groupreduce(SquareCube(), (Types.INT, Types.STRING))

```

This would would return the number of occurrences of a word in a group.

Groups can also be sorted before applying a `group_reduce` operation, like this:

```python
data \
  .group_by(0) \
  .sort_by(1) \
  .groupreduce(SquareCube(), (Types.INT, Types.STRING))
```
Note: A GroupSort often comes for free if the grouping is established using a sort-based execution strategy of an operator before the reduce operation.

#### Join Operation

The Join transformation joins two DataSets into one DataSet. The elements of 
both DataSets are joined on one or more keys which can be specified using
one or more field position keys.

There are a few different ways to perform a Join transformation which are shown in the following.

##### Join with JoinFunction
```python
class JoinCounts(JoinFunction):
  def join(self, value1, value2):
    word = value1[0]
    count = value1[1] + value2[1]
    return (count, word)

data1 \
  .join(data2) \
  .where(0) \
  .equal_to(0) \
  .using(JoinCounts(), (Types.INT, Types.STRING))

```
This code would for example join two separately generated WordCount DataSets.

##### Join with Projection

```python
data1 \
  .join(data2) \
  .where(0) \
  .equal_to(0) \
  .project_first(0,1,2) \
  .project_second(1) \
  .types(<types>)
```
This code would take the first 3 columns of the first set and the second
column of the second set to create a new one.

##### Join with DataSet Size Hint
In order to guide the optimizer to pick the right execution strategy, 
you can hint the size of a DataSet to join using `join_with_tiny()` or
`join_with_huge()` instead of `join()`.

#### Cross Operation

The Cross transformation combines two DataSets into one DataSet. It builds 
all pairwise combinations of the elements of both input DataSets, i.e., it 
builds a Cartesian product. The Cross transformation either calls a user-defined 
CrossFunction on each pair of elements or applies a projection. 
Both modes are shown in the following.

##### Cross with User-Defined Function
```python
class Cartesian(CrossFunction):
  def cross(self, value1, value2):
    return (value1, value2)

data1 \
  .cross(data2) \
  .using(Cartesian())
```
This code would create all pairings of both DataSets.

##### Cross with Projection
```python
data1 \
  .project_first(0,1) \
  .projet_second(2) \
  .types(<types>)
```
This code would take the first 2 columns of the first set and the third 
column of the second set to create a new one.

##### Cross with DataSet Size Hint
Similar to the join operation, you can hint the size of a DataSet to join 
using `cross_with_tiny()` or `cross_with_huge()` instead of `cross()`.

#### CoGroup Operation

The CoGroup transformation jointly processes groups of two DataSets. Both DataSets 
are grouped on a defined key and groups of both DataSets that share the same key are 
handed together to a user-defined CoGroupFunction. If for a specific key only one 
DataSet has a group, the CoGroupFunction is called with this group and an empty group. 
A CoGroupFunction can separately iterate over the elements of both groups and return 
an arbitrary number of result elements.

Similar to Reduce, GroupReduce, and Join, keys can be defined using one or more field 
position keys.

#### Project

The Project transformation removes or moves Tuple fields of a Tuple DataSet. The project(int...) 
method selects Tuple fields that should be retained by their index and defines their order 
in the output Tuple. The `types(<types>)` method must give the types of the output Tuple fields.

Projections do not require the definition of a user function.

```python
data \
  .project(1,0) \
  .types(<types>)
```
This would create a new DataSet containing the secondd and first column of `data`.

#### Union

When you want to have the combination of several data sets as the input of
an operation you can use a union to combine them. It is used like this

```python
union = data1.union(data2)
```

<section id="data-sinks">
Creating Data Sinks
-------------------

To output a DataSet one of the following methods is used:

```python
data.write_text("<file-path>")
data.write_csv("<file-path>")
data.output()
```

The file path can be on of either `file:///some/file` to acces files on the
local machine or `hdfs://some/path` to read files from HDFS.
We will now have a look at each methoddhow how they are employed and in
which situations.

<section id="text-output">
#### write_text()

This method simply creates a text file and writes every `String` to each line. 
It is used as:
```python
data.write_text("<file-path>", WriteMode)
```
The `data` would then be a `DataSet[String]`.
`WriteMode`is an optional parameter indicatingh whether an existing file for
`file-path` should be overwritten. (default: NO_OVERWRITE)

<section id="csv-output">
#### write_csv()

This method is used to write Csv-Files, as the name suggests. Every value is
written in a separate line; the values in each line are separated by a comma.

```python
data.write_csv("<file-path>", WriteMode)
```
`WriteMode`is an optional parameter indicating whether an existing file for
`file-path` should be overwritten. (default: NO_OVERWRITE)

<section id="print-output">
#### output()

This method write every value to the console. The data output is ddone entirely
on the Java side, as such it Java's toString() method is used on non-Strings.
It is used as:

```python
data.output()
```

<section id="debugging">
Debugging
--------------

When debugging your program, do not use statements that involve `stdout` or `stderr`,
like `print()`. Instead write directly to a file.


<section id="execution">
Executing Jobs
--------------

There are two ways one can execute a data flow plan: local execution and
remote/cluster execution. When using local execution the plan is executed on
the local computer. This is handy while developing jobs because you can
easily debug your code and iterate quickly. When a job is ready to be
used on bigger data sets it can be executed on a cluster. We will
now give an example for each of the two execution modes.

Inside the plan, the only difference between local and cluster mode is the call
to `env.execute()` call, which for local mode has to be modifiedd to `env.execute(local=true)`.

To run the plan with Flink, go to your Flink distribution, and run the 
pysphere.sh script from the /bin folder. This script takes two file paths as arguments: 
the path to the folder which contains all files related to the plan, and the path to the plan itself.
