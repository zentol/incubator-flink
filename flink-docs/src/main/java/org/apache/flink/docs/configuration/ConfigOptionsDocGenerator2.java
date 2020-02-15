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

package org.apache.flink.docs.configuration;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.Formatter;
import org.apache.flink.configuration.description.HtmlFormatter;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.docs.util.Utils.escapeCharacters;

/**
 * Class used for generating code based documentation of configuration parameters.
 */
public class ConfigOptionsDocGenerator2 {

	private static final Logger LOG = LoggerFactory.getLogger(ConfigOptionsDocGenerator2.class);

	static final OptionsClassLocation[] LOCATIONS = new OptionsClassLocation[]{
		new OptionsClassLocation("flink-core", "org.apache.flink.configuration"),
		new OptionsClassLocation("flink-runtime", "org.apache.flink.runtime.shuffle"),
		new OptionsClassLocation("flink-runtime", "org.apache.flink.runtime.jobgraph"),
		new OptionsClassLocation("flink-streaming-java", "org.apache.flink.streaming.api.environment"),
		new OptionsClassLocation("flink-yarn", "org.apache.flink.yarn.configuration"),
		new OptionsClassLocation("flink-mesos", "org.apache.flink.mesos.configuration"),
		new OptionsClassLocation("flink-mesos", "org.apache.flink.mesos.runtime.clusterframework"),
		new OptionsClassLocation("flink-metrics/flink-metrics-prometheus", "org.apache.flink.metrics.prometheus"),
		new OptionsClassLocation("flink-metrics/flink-metrics-influxdb", "org.apache.flink.metrics.influxdb"),
		new OptionsClassLocation("flink-state-backends/flink-statebackend-rocksdb", "org.apache.flink.contrib.streaming.state"),
		new OptionsClassLocation("flink-table/flink-table-api-java", "org.apache.flink.table.api.config"),
		new OptionsClassLocation("flink-python", "org.apache.flink.python"),
		new OptionsClassLocation("flink-kubernetes", "org.apache.flink.kubernetes.configuration")
	};

	static final Set<String> EXCLUSIONS = new HashSet<>(Arrays.asList(
		"org.apache.flink.configuration.ReadableConfig",
		"org.apache.flink.configuration.WritableConfig",
		"org.apache.flink.configuration.ConfigOptions",
		"org.apache.flink.streaming.api.environment.CheckpointConfig",
		"org.apache.flink.contrib.streaming.state.PredefinedOptions",
		"org.apache.flink.python.PythonConfig"));

	static final String DEFAULT_PATH_PREFIX = "src/main/java";

	private static final String CLASS_NAME_GROUP = "className";
	private static final String CLASS_PREFIX_GROUP = "classPrefix";
	private static final Pattern CLASS_NAME_PATTERN = Pattern.compile("(?<" + CLASS_NAME_GROUP + ">(?<" + CLASS_PREFIX_GROUP + ">[a-zA-Z]*)(?:Options|Config|Parameters))(?:\\.java)?");

	private static final Formatter formatter = new HtmlFormatter();

	/**
	 * This method generates html tables from set of classes containing {@link ConfigOption ConfigOptions}.
	 *
	 * <p>For each class 1 or more html tables will be generated and placed into a separate file, depending on whether
	 * the class is annotated with {@link ConfigGroups}. The tables contain the key, default value and description for
	 * every {@link ConfigOption}.
	 *
	 * <p>One additional table is generated containing all {@link ConfigOption ConfigOptions} that are annotated with
	 * {@link Documentation.Section}.
	 *
	 * @param args [0] output directory for the generated files
	 *             [1] project root directory
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String outputDirectory = args[0];
		String rootDir = args[1];

		LOG.info("Searching the following locations; configured via {}#LOCATIONS:{}",
			ConfigOptionsDocGenerator2.class.getCanonicalName(),
			Arrays.stream(LOCATIONS).map(OptionsClassLocation::toString).collect(Collectors.joining("\n\t", "\n\t", "")));
		LOG.info("Excluding the following classes; configured via {}#EXCLUSIONS:{}",
			ConfigOptionsDocGenerator2.class.getCanonicalName(),
			EXCLUSIONS.stream().collect(Collectors.joining("\n\t", "\n\t", "")));

		List<UnassignedOption> optionsToDocument = discoverOptions(rootDir, LOCATIONS, DEFAULT_PATH_PREFIX)
			.filter(option -> !option.isDeprecated() && !option.shouldBeExcluded())
			.collect(Collectors.toList());

		generateSections(optionsToDocument, outputDirectory);
	}

	private static class OptionWithMetaInfo {
		private final ConfigOption<?> configOption;
		protected final Field field;

		OptionWithMetaInfo(ConfigOption<?> configOption, Field field) {
			this.configOption = configOption;
			this.field = field;
		}

		public ConfigOption<?> getConfigOption() {
			return configOption;
		}

		public boolean isDeprecated() {
			return field.getAnnotation(Deprecated.class) != null;
		}

		public boolean shouldBeExcluded() {
			return field.getAnnotation(Documentation.ExcludeFromDocumentation.class) != null;
		}

		public Optional<String> getDefaultOverride() {
			return Optional.ofNullable(field.getAnnotation(Documentation.OverrideDefault.class)).map(Documentation.OverrideDefault::value);
		}

		public Optional<Documentation.ExecMode> getTableExecutionMode() {
			return Optional.ofNullable(field.getAnnotation(Documentation.TableOption.class)).map(Documentation.TableOption::execMode);
		}

		public UnassignedOption withSections(Set<Section> sections) {
			return new UnassignedOption(configOption, sections, field);
		}

	}

	private static class UnassignedOption extends OptionWithMetaInfo {
		private final Set<Section> sections;

		private UnassignedOption(ConfigOption<?> configOption, Set<Section> sections, Field field) {
			super(configOption, field);
			this.sections = sections;
		}

		public Set<Section> getSections() {
			return sections;
		}

		public AssignedOption pin(Section section) {
			return new AssignedOption(getConfigOption(), section, field);
		}
	}

	private static class AssignedOption extends OptionWithMetaInfo {
		private final Section section;

		private AssignedOption(ConfigOption<?> configOption, Section section, Field field) {
			super(configOption, field);
			this.section = section;
		}

		public Section getSection() {
			return section;
		}
	}

	private static class Section {
		private final String name;
		private final int position;

		public Section(String name, int position) {
			this.name = name;
			this.position = position;
		}
	}

	private static Stream<Section> getSections(Field field) {
		Documentation.Section sectionAnnotation = field.getAnnotation(Documentation.Section.class);
		return sectionAnnotation != null
			? Arrays.stream(sectionAnnotation.value()).map(s -> new Section(s, sectionAnnotation.position()))
			: Stream.empty();
	}

	private static Stream<Section> asSection(Optional<ConfigGroup> group, Class<?> optionsClass) {
		final String rawSectionName = group.map(ConfigGroup::name).orElse(optionsClass.getSimpleName());
		return Stream.of(new Section(rawSectionName.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase(), 0));
	}

	private static UnassignedOption normalize(OptionWithMetaInfo optionWithMetaInfo, Optional<ConfigGroup> group, Class<?> optionsClass) {
		Set<Section> sections = Stream.concat(getSections(optionWithMetaInfo.field), asSection(group, optionsClass)).collect(Collectors.toSet());

		return optionWithMetaInfo.withSections(sections);
	}

	private static Stream<UnassignedOption> discoverOptions(String rootDir, OptionsClassLocation[] locations, String pathPrefix) throws IOException, ClassNotFoundException {
		final List<UnassignedOption> options = new ArrayList<>();

		for (OptionsClassLocation location : locations) {

			processConfigOptions(rootDir, location.getModule(), location.getPackage(), pathPrefix, optionsClass -> {
				ConfigGroups configGroups = optionsClass.getAnnotation(ConfigGroups.class);
				List<OptionWithMetaInfo> allOptions = extractConfigOptions(optionsClass);

				if (configGroups != null) {
					Tree tree = new Tree(configGroups.groups(), allOptions);

					for (ConfigGroup group : configGroups.groups()) {
						tree.findConfigOptions(group).stream()
							.map(option -> normalize(option, Optional.of(group), optionsClass))
							.forEach(options::add);
					}
					tree.getDefaultOptions().stream()
						.map(option -> normalize(option, Optional.empty(), optionsClass))
						.forEach(options::add);
				} else {
					allOptions.stream().map(option -> normalize(option, Optional.empty(), optionsClass))
						.forEach(options::add);
				}
			});
		}

		return options.stream();
	}

	@VisibleForTesting
	static void generateSections(Collection<UnassignedOption> allOptions, String outputDirectory) {
		Map<String, List<AssignedOption>> optionsGroupedBySection = allOptions.stream()
			.flatMap(option -> option.getSections().stream().map(section -> Tuple2.of(section, option)))
			.collect(Collectors.groupingBy(
				option -> option.f0.name,
				Collectors.mapping(option -> option.f1.pin(option.f0), Collectors.toList())));

		optionsGroupedBySection.forEach(
			(section, options) -> {
				options.sort((o1, o2) -> {
					int position1 = o1.section.position;
					int position2 = o2.section.position;
					if (position1 == position2) {
						return o1.getConfigOption().key().compareTo(o2.getConfigOption().key());
					} else {
						return Integer.compare(position1, position2);
					}
				});

				String sectionHtmlTable = toHtmlTable(options);
				try {
					Files.write(Paths.get(outputDirectory, getSectionFileName(section)), sectionHtmlTable.getBytes(StandardCharsets.UTF_8));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		);
	}

	@VisibleForTesting
	static String getSectionFileName(String section) {
		return section + "_section.html";
	}

	@VisibleForTesting
	static void processConfigOptions(String rootDir, String module, String packageName, String pathPrefix, ThrowingConsumer<Class<?>, IOException> classConsumer) throws IOException, ClassNotFoundException {
		Path configDir = Paths.get(rootDir, module, pathPrefix, packageName.replaceAll("\\.", "/"));

		try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir)) {
			for (Path entry : stream) {
				String fileName = entry.getFileName().toString();
				Matcher matcher = CLASS_NAME_PATTERN.matcher(fileName);
				if (matcher.matches()) {
					final String className = packageName + '.' + matcher.group(CLASS_NAME_GROUP);

					if (!EXCLUSIONS.contains(className)) {
						Class<?> optionsClass = Class.forName(className);
						classConsumer.accept(optionsClass);
					}
				}
			}
		}
	}

	@VisibleForTesting
	static List<OptionWithMetaInfo> extractConfigOptions(Class<?> clazz) {
		try {
			List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
			Field[] fields = clazz.getFields();
			for (Field field : fields) {
				if (isConfigOption(field)) {
					configOptions.add(new OptionWithMetaInfo((ConfigOption<?>) field.get(null), field));
				}
			}

			return configOptions;
		} catch (Exception e) {
			throw new RuntimeException("Failed to extract config options from class " + clazz + '.', e);
		}
	}

	private static boolean isConfigOption(Field field) {
		return field.getType().equals(ConfigOption.class);
	}

	/**
	 * Transforms this configuration group into HTML formatted table.
	 * Options are sorted alphabetically by key.
	 *
	 * @param options list of options to include in this group
	 * @return string containing HTML formatted table
	 */
	private static String toHtmlTable(final List<AssignedOption> options) {
		StringBuilder htmlTable = new StringBuilder();
		htmlTable.append("<table class=\"table table-bordered\">\n");
		htmlTable.append("    <thead>\n");
		htmlTable.append("        <tr>\n");
		htmlTable.append("            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n");
		htmlTable.append("            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n");
		htmlTable.append("            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n");
		htmlTable.append("            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n");
		htmlTable.append("        </tr>\n");
		htmlTable.append("    </thead>\n");
		htmlTable.append("    <tbody>\n");

		for (AssignedOption option : options) {
			htmlTable.append(toHtmlString(option));
		}

		htmlTable.append("    </tbody>\n");
		htmlTable.append("</table>\n");

		return htmlTable.toString();
	}

	/**
	 * Transforms option to table row.
	 *
	 * @param optionWithMetaInfo option to transform
	 * @return row with the option description
	 */
	private static String toHtmlString(final AssignedOption optionWithMetaInfo) {
		ConfigOption<?> option = optionWithMetaInfo.getConfigOption();
		String defaultValue = stringifyDefault(optionWithMetaInfo);
		String type = typeToHtml(optionWithMetaInfo);
		StringBuilder execModeStringBuilder = new StringBuilder();

		Optional<Documentation.ExecMode> tableExecutionMode = optionWithMetaInfo.getTableExecutionMode();
		if (tableExecutionMode.isPresent()) {
			Documentation.ExecMode execMode = tableExecutionMode.get();
			if (Documentation.ExecMode.BATCH_STREAMING.equals(execMode)) {
				execModeStringBuilder.append("<br> <span class=\"label label-primary\">")
					.append(Documentation.ExecMode.BATCH.toString())
					.append("</span> <span class=\"label label-primary\">")
					.append(Documentation.ExecMode.STREAMING.toString())
					.append("</span>");
			} else {
				execModeStringBuilder.append("<br> <span class=\"label label-primary\">")
					.append(execMode.toString())
					.append("</span>");
			}
		}

		return "" +
			"        <tr>\n" +
			"            <td><h5>" + escapeCharacters(option.key()) + "</h5>" + execModeStringBuilder.toString() + "</td>\n" +
			"            <td style=\"word-wrap: break-word;\">" + escapeCharacters(addWordBreakOpportunities(defaultValue)) + "</td>\n" +
			"            <td>" + type + "</td>\n" +
			"            <td>" + formatter.format(option.description()) + "</td>\n" +
			"        </tr>\n";
	}

	private static Class<?> getClazz(ConfigOption<?> option) {
		try {
			Method getClazzMethod = ConfigOption.class.getDeclaredMethod("getClazz");
			getClazzMethod.setAccessible(true);
			Class clazz = (Class) getClazzMethod.invoke(option);
			getClazzMethod.setAccessible(false);
			return clazz;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static boolean isList(ConfigOption<?> option) {
		try {
			Method getClazzMethod = ConfigOption.class.getDeclaredMethod("isList");
			getClazzMethod.setAccessible(true);
			boolean isList = (boolean) getClazzMethod.invoke(option);
			getClazzMethod.setAccessible(false);
			return isList;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@VisibleForTesting
	static String typeToHtml(OptionWithMetaInfo optionWithMetaInfo) {
		ConfigOption<?> option = optionWithMetaInfo.getConfigOption();
		Class<?> clazz = getClazz(option);
		boolean isList = isList(option);

		if (clazz.isEnum()) {
			return enumTypeToHtml(clazz, isList);
		}

		return atomicTypeToHtml(clazz, isList);
	}

	private static String atomicTypeToHtml(Class<?> clazz, boolean isList) {
		String typeName = clazz.getSimpleName();

		final String type;
		if (isList) {
			type = String.format("List<%s>", typeName);
		} else {
			type = typeName;
		}

		return escapeCharacters(type);
	}

	private static String enumTypeToHtml(Class<?> enumClazz, boolean isList) {
		final String type;
		if (isList) {
			type = "List<Enum>";
		} else {
			type = "Enum";
		}

		return String.format(
			"<p>%s</p>Possible values: %s",
			escapeCharacters(type),
			escapeCharacters(Arrays.toString(enumClazz.getEnumConstants())));
	}

	@VisibleForTesting
	static String stringifyDefault(OptionWithMetaInfo optionWithMetaInfo) {
		return optionWithMetaInfo
			.getDefaultOverride()
			.orElse(stringifyObject(optionWithMetaInfo.configOption.defaultValue()));
	}

	@SuppressWarnings("unchecked")
	private static String stringifyObject(Object value) {
		if (value instanceof String) {
			if (((String) value).isEmpty()) {
				return "(none)";
			}
			return "\"" + value + "\"";
		} else if (value instanceof Duration) {
			return TimeUtils.formatWithHighestUnit((Duration) value);
		} else if (value instanceof List) {
			return ((List<Object>) value).stream()
				.map(ConfigOptionsDocGenerator2::stringifyObject)
				.collect(Collectors.joining(";"));
		} else if (value instanceof Map) {
			return ((Map<String, String>) value)
				.entrySet()
				.stream()
				.map(e -> String.format("%s:%s", e.getKey(), e.getValue()))
				.collect(Collectors.joining(","));
		}
		return value == null ? "(none)" : value.toString();
	}

	private static String addWordBreakOpportunities(String value) {
		return value
			// allow breaking of semicolon separated lists
			.replace(";", ";<wbr>");
	}

	/**
	 * Data structure used to assign {@link ConfigOption ConfigOptions} to the {@link ConfigGroup} with the longest
	 * matching prefix.
	 */
	private static class Tree {
		private final Node root = new Node();

		Tree(ConfigGroup[] groups, Collection<OptionWithMetaInfo> options) {
			// generate a tree based on all key prefixes
			for (ConfigGroup group : groups) {
				String[] keyComponents = group.keyPrefix().split("\\.");
				Node currentNode = root;
				for (String keyComponent : keyComponents) {
					currentNode = currentNode.addChild(keyComponent);
				}
				currentNode.markAsGroupRoot();
			}

			// assign options to their corresponding group, i.e. the last group root node encountered when traversing
			// the tree based on the option key
			for (OptionWithMetaInfo option : options) {
				findGroupRoot(option.getConfigOption().key()).assignOption(option);
			}
		}

		List<OptionWithMetaInfo> findConfigOptions(ConfigGroup configGroup) {
			Node groupRoot = findGroupRoot(configGroup.keyPrefix());
			return groupRoot.getConfigOptions();
		}

		List<OptionWithMetaInfo> getDefaultOptions() {
			return root.getConfigOptions();
		}

		private Node findGroupRoot(String key) {
			String[] keyComponents = key.split("\\.");
			Node lastRootNode = root;
			Node currentNode = root;
			for (String keyComponent : keyComponents) {
				final Node childNode = currentNode.getChild(keyComponent);
				if (childNode == null) {
					break;
				} else {
					currentNode = childNode;
					if (currentNode.isGroupRoot()) {
						lastRootNode = currentNode;
					}
				}
			}
			return lastRootNode;
		}

		private static class Node {
			private final List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
			private final Map<String, Node> children = new HashMap<>(8);
			private boolean isGroupRoot = false;

			private Node addChild(String keyComponent) {
				Node child = children.get(keyComponent);
				if (child == null) {
					child = new Node();
					children.put(keyComponent, child);
				}
				return child;
			}

			private Node getChild(String keyComponent) {
				return children.get(keyComponent);
			}

			private void assignOption(OptionWithMetaInfo option) {
				configOptions.add(option);
			}

			private boolean isGroupRoot() {
				return isGroupRoot;
			}

			private void markAsGroupRoot() {
				this.isGroupRoot = true;
			}

			private List<OptionWithMetaInfo> getConfigOptions() {
				return configOptions;
			}
		}
	}

	private ConfigOptionsDocGenerator2() {
	}
}
