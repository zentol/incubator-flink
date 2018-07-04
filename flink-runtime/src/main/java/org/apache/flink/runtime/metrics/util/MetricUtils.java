/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class to register pre-defined metric sets.
 */
public class MetricUtils {
	private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);
	private static final String METRIC_GROUP_STATUS_NAME = "Status";

	private MetricUtils() {
	}

	public static JobManagerMetricGroup instantiateJobManagerMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostname) {
		final JobManagerMetricGroup jobManagerMetricGroup = new JobManagerMetricGroup(
			metricRegistry,
			hostname);

		MetricGroup statusGroup = jobManagerMetricGroup.addGroup(METRIC_GROUP_STATUS_NAME);

		// initialize the JM metrics
		instantiateStatusMetrics(statusGroup);

		return jobManagerMetricGroup;
	}

	public static TaskManagerMetricGroup instantiateTaskManagerMetricGroup(
			MetricRegistry metricRegistry,
			TaskManagerLocation taskManagerLocation,
			NetworkEnvironment network) {
		final TaskManagerMetricGroup taskManagerMetricGroup = new TaskManagerMetricGroup(
			metricRegistry,
			taskManagerLocation.getHostname(),
			taskManagerLocation.getResourceID().toString());

		MetricGroup statusGroup = taskManagerMetricGroup.addGroup(METRIC_GROUP_STATUS_NAME);

		// Initialize the TM metrics
		instantiateStatusMetrics(statusGroup);

		MetricGroup networkGroup = statusGroup
			.addGroup("Network");
		instantiateNetworkMetrics(networkGroup, network);

		return taskManagerMetricGroup;
	}

	public static void instantiateStatusMetrics(MetricGroup metricGroup) {
		JvmMetricsContainer.registerStatusMetrics(metricGroup);
	}

	private static void instantiateNetworkMetrics(
		MetricGroup metrics,
		final NetworkEnvironment network) {
		metrics.<Long, Gauge<Long>>gauge("TotalMemorySegments", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return (long) network.getNetworkBufferPool().getTotalNumberOfMemorySegments();
			}
		});
	}

	private enum JvmMetricsContainer {
		;

		public static void registerStatusMetrics(MetricGroup metricGroup) {
			MetricGroup jvm = metricGroup.addGroup("JVM");

			ClassLoaderMetrics.registerClassLoaderMetrics(jvm.addGroup("ClassLoader"));
			GarbageCollectorMetrics.registerGarbageCollectorMetrics(jvm.addGroup("GarbageCollector"));
			MemoryMetrics.registerMemoryMetrics(jvm.addGroup("Memory"));
			ThreadMetrics.registerThreadMetrics(jvm.addGroup("Threads"));
			CpuMetrics.registerCPUMetrics(jvm.addGroup("CPU"));
		}

		private enum ClassLoaderMetrics {
			;

			private static final Gauge<Long> CLASSES_LOADED;
			private static final Gauge<Long> CLASSES_UNLOADED;

			static {
				final ClassLoadingMXBean mxBean = ManagementFactory.getClassLoadingMXBean();

				CLASSES_LOADED = mxBean::getTotalLoadedClassCount;
				CLASSES_UNLOADED = mxBean::getUnloadedClassCount;
			}

			private static void registerClassLoaderMetrics(MetricGroup metrics) {
				metrics.gauge("ClassesLoaded", CLASSES_LOADED);
				metrics.gauge("ClassesUnloaded", CLASSES_UNLOADED);
			}
		}

		private enum GarbageCollectorMetrics {
			;

			private static final List<Tuple2<String, Tuple2<Gauge<Long>, Gauge<Long>>>> METRICS;

			static {
				List<Tuple2<String, Tuple2<Gauge<Long>, Gauge<Long>>>> metrics = new ArrayList<>(4);
				List<GarbageCollectorMXBean> garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();

				for (final GarbageCollectorMXBean garbageCollector : garbageCollectors) {
					metrics.add(Tuple2.of(garbageCollector.getName(), Tuple2.of(garbageCollector::getCollectionCount, garbageCollector::getCollectionTime)));
				}
				METRICS = Collections.unmodifiableList(metrics);
			}

			private static void registerGarbageCollectorMetrics(MetricGroup metrics) {
				for (final Tuple2<String, Tuple2<Gauge<Long>, Gauge<Long>>> metric : METRICS) {
					MetricGroup gcGroup = metrics.addGroup(metric.f0);

					gcGroup.gauge("Count", metric.f1.f0);
					gcGroup.gauge("Time", metric.f1.f1);
				}
			}
		}

		private enum MemoryMetrics {
			;

			private static final Gauge<Long> HEAP_MEMORY_USED;
			private static final Gauge<Long> HEAP_MEMORY_COMMITTED;
			private static final Gauge<Long> HEAP_MEMORY_MAX;

			private static final Gauge<Long> NON_HEAP_MEMORY_USED;
			private static final Gauge<Long> NON_HEAP_MEMORY_COMMITTED;
			private static final Gauge<Long> NON_HEAP_MEMORY_MAX;

			private static final Gauge<Long> DIRECT_COUNT;
			private static final Gauge<Long> DIRECT_MEMORY_USED;
			private static final Gauge<Long> DIRECT_TOTAL_CAPACITY;

			private static final Gauge<Long> MAPPED_COUNT;
			private static final Gauge<Long> MAPPED_MEMORY_USED;
			private static final Gauge<Long> MAPPED_TOTAL_CAPACITY;

			static {
				final MemoryMXBean mxBean = ManagementFactory.getMemoryMXBean();

				HEAP_MEMORY_USED = mxBean.getHeapMemoryUsage()::getUsed;
				HEAP_MEMORY_COMMITTED = mxBean.getHeapMemoryUsage()::getCommitted;
				HEAP_MEMORY_MAX = mxBean.getHeapMemoryUsage()::getMax;

				NON_HEAP_MEMORY_USED = mxBean.getNonHeapMemoryUsage()::getUsed;
				NON_HEAP_MEMORY_COMMITTED = mxBean.getNonHeapMemoryUsage()::getCommitted;
				NON_HEAP_MEMORY_MAX = mxBean.getNonHeapMemoryUsage()::getMax;

				final MBeanServer con = ManagementFactory.getPlatformMBeanServer();

				final String directBufferPoolName = "java.nio:type=BufferPool,name=direct";

				Gauge<Long> directCount = null;
				Gauge<Long> directMemoryUsed = null;
				Gauge<Long> directTotalCapacity = null;
				try {
					final ObjectName directObjectName = new ObjectName(directBufferPoolName);

					directCount = new AttributeGauge<>(con, directObjectName, "Count", -1L);
					directMemoryUsed = new AttributeGauge<>(con, directObjectName, "MemoryUsed", -1L);
					directTotalCapacity = new AttributeGauge<>(con, directObjectName, "TotalCapacity", -1L);

				} catch (MalformedObjectNameException e) {
					LOG.warn("Could not create object name {}.", directBufferPoolName, e);
				}

				DIRECT_COUNT = directCount;
				DIRECT_MEMORY_USED = directMemoryUsed;
				DIRECT_TOTAL_CAPACITY = directTotalCapacity;

				final String mappedBufferPoolName = "java.nio:type=BufferPool,name=mapped";

				Gauge<Long> mappedCount = null;
				Gauge<Long> mappedMemoryUsed = null;
				Gauge<Long> mappedTotalCapacity = null;
				try {
					final ObjectName mappedObjectName = new ObjectName(mappedBufferPoolName);

					mappedCount = new AttributeGauge<>(con, mappedObjectName, "Count", -1L);
					mappedMemoryUsed = new AttributeGauge<>(con, mappedObjectName, "MemoryUsed", -1L);
					mappedTotalCapacity = new AttributeGauge<>(con, mappedObjectName, "TotalCapacity", -1L);

				} catch (MalformedObjectNameException e) {
					LOG.warn("Could not create object name {}.", mappedBufferPoolName, e);
				}

				MAPPED_COUNT = mappedCount;
				MAPPED_MEMORY_USED = mappedMemoryUsed;
				MAPPED_TOTAL_CAPACITY = mappedTotalCapacity;
			}

			private static void registerMemoryMetrics(MetricGroup metrics) {
				MetricGroup heap = metrics.addGroup("Heap");

				heap.gauge("Used", HEAP_MEMORY_USED);
				heap.gauge("Committed", HEAP_MEMORY_COMMITTED);
				heap.gauge("Max", HEAP_MEMORY_MAX);

				MetricGroup nonHeap = metrics.addGroup("NonHeap");

				nonHeap.gauge("Used", NON_HEAP_MEMORY_USED);
				nonHeap.gauge("Committed", NON_HEAP_MEMORY_COMMITTED);
				nonHeap.gauge("Max", NON_HEAP_MEMORY_MAX);

				MetricGroup direct = metrics.addGroup("Direct");
				if (DIRECT_COUNT != null) {
					direct.gauge("Count", DIRECT_COUNT);
				}
				if (DIRECT_MEMORY_USED != null) {
					direct.gauge("MemoryUsed", DIRECT_MEMORY_USED);
				}
				if (DIRECT_TOTAL_CAPACITY != null) {
					direct.gauge("TotalCapacity", DIRECT_TOTAL_CAPACITY);
				}

				MetricGroup mapped = metrics.addGroup("Mapped");
				if (MAPPED_COUNT != null) {
					mapped.gauge("Count", MAPPED_COUNT);
				}
				if (MAPPED_MEMORY_USED != null) {
					mapped.gauge("MemoryUsed", MAPPED_MEMORY_USED);
				}
				if (MAPPED_TOTAL_CAPACITY != null) {
					mapped.gauge("TotalCapacity", MAPPED_TOTAL_CAPACITY);
				}
			}
		}

		private enum ThreadMetrics {
			;

			private static final Gauge<Integer> THREAD_COUNT;

			static {
				final ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

				THREAD_COUNT = mxBean::getThreadCount;
			}

			private static void registerThreadMetrics(MetricGroup metrics) {
				metrics.<Integer, Gauge<Integer>>gauge("Count", THREAD_COUNT);
			}
		}

		private enum CpuMetrics {
			;

			private static final Gauge<Double> CPU_PROCESS_LOAD;
			private static final Gauge<Long> CPU_PROCESS_TIME;

			static {
				Gauge<Double> cpuProcessLoad = null;
				Gauge<Long> cpuProcessTime = null;
				try {
					final com.sun.management.OperatingSystemMXBean mxBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

					cpuProcessLoad = mxBean::getProcessCpuLoad;
					cpuProcessTime = mxBean::getProcessCpuTime;
				} catch (Exception e) {
					LOG.warn("Cannot access com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()" +
						" - CPU load metrics will not be available.", e);
				}
				CPU_PROCESS_LOAD = cpuProcessLoad;
				CPU_PROCESS_TIME = cpuProcessTime;
			}

			private static void registerCPUMetrics(MetricGroup group) {
				if (CPU_PROCESS_LOAD != null) {
					group.gauge("Load", CPU_PROCESS_LOAD);
				}
				if (CPU_PROCESS_TIME != null) {
					group.gauge("Time", CPU_PROCESS_TIME);
				}
			}
		}
	}

	private static final class AttributeGauge<T> implements Gauge<T> {
		private final MBeanServer server;
		private final ObjectName objectName;
		private final String attributeName;
		private final T errorValue;

		private AttributeGauge(MBeanServer server, ObjectName objectName, String attributeName, T errorValue) {
			this.server = Preconditions.checkNotNull(server);
			this.objectName = Preconditions.checkNotNull(objectName);
			this.attributeName = Preconditions.checkNotNull(attributeName);
			this.errorValue = errorValue;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T getValue() {
			try {
				return (T) server.getAttribute(objectName, attributeName);
			} catch (MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException e) {
				LOG.warn("Could not read attribute {}.", attributeName, e);
				return errorValue;
			}
		}
	}
}
