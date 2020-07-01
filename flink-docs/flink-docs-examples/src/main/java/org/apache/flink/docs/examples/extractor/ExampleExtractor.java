/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.docs.examples.extractor;

import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TODO: Add javadoc.
 */
public class ExampleExtractor {
	private static final Logger LOG = LoggerFactory.getLogger(ExampleExtractor.class);

	private static final Pattern START_COMMENT_PATTERN = Pattern.compile("\t*// <(?<name> ([\\w ]+;?)*)?");
	private static final Pattern DIRECTIVE_COMMENT_PATTERN = Pattern.compile("\t*// \\| ?(([\\w-]*)(:('[\\w ]+'))? ?)+");
	private static final Pattern DIRECTIVE_PATTERN = Pattern.compile("(?<directive>[\\w-]+)(?::'(?<arg>[\\w ]+)')?");

	public static void main(String[] args) throws IOException {
		Path docsExamplesDir = Paths.get(args[0]);
		Path outputDirectory = Paths.get(args[1]);

		if (Files.exists(outputDirectory)) {
			FileUtils.listFilesInDirectory(outputDirectory, ignored -> true)
				.forEach(file -> {
					try {
						Files.delete(file);
					} catch (IOException e) {
						LOG.warn("Error while deleting file.", e);
					}
				});
		}

		internalMain(docsExamplesDir, outputDirectory.resolve("java"), Language.JAVA);
		internalMain(docsExamplesDir, outputDirectory.resolve("scala"), Language.SCALA);
	}

	private static void internalMain(Path baseDirectory, Path outputDirectory, Language language) throws IOException {
		Path exampleDirectory = baseDirectory.resolve(language.getRelativeExampleDirectory());
		if (!Files.exists(exampleDirectory)) {
			LOG.warn("Skipping {}-language directory '{}' because it does not exist.", language.getMarkdownLanguageIdentifier(), exampleDirectory);
			return;
		}
		try (Stream<Path> exampleFiles = Files.walk(exampleDirectory)) {
			exampleFiles
				.filter(language.getExampleFileFilter())
				.forEach(file -> {
					List<Block> lists = processFile(file);
					String fileName = FilenameUtils.removeExtension(file.getFileName().toString())
						.replaceFirst("(.*)Example", "$1");
					lists.forEach(block -> write(fileName, block, language, outputDirectory));
				});
		}
	}

	private static List<Block> processFile(Path file) {
		if (Files.isDirectory(file)) {
			return Collections.emptyList();
		}
		try {
			return processFile(Files.readAllLines(file));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static List<Block> processFile(List<String> lines) {
		List<Block> blocks = new ArrayList<>();
		Queue<Integer> blockStarts = new ArrayDeque<>();
		for (int x = 0; x < lines.size(); x++) {
			String line = lines.get(x);
			if (isBlockStart(line)) {
				blockStarts.add(x);
			} else if (isBlockEnd(line)) {
				List<String> rawBlock = lines.subList(blockStarts.remove(), x + 1);

				List<String> names = extractBlockNames(rawBlock.get(0));

				List<String> strings = processLines(rawBlock.subList(1, rawBlock.size() - 1));

				names.forEach(name -> blocks.add(new Block(name.equals("_") ? null : name, strings)));
			}
		}
		return mergeBlocks(blocks);
	}

	private static List<String> extractBlockNames(String start) {
		Matcher matcher = START_COMMENT_PATTERN.matcher(start);
		if (!matcher.matches()) {
			throw new RuntimeException(String.format("Start comment: '%s' did not conform to expected format '%s'.", start, START_COMMENT_PATTERN.pattern()));
		}

		String rawNames = matcher.group("name");

		if (rawNames == null) {
			return Collections.singletonList("_");
		} else {
			String[] split = rawNames.split(";");
			List<String> names = new ArrayList<>();
			for (String s : split) {
				names.add(s.trim());
			}
			return names;
		}
	}

	private static List<String> processLines(List<String> codeLines) {
		codeLines = sanitizeBLock(codeLines);
		codeLines = alignLeft(codeLines);
		codeLines = processDirectives(codeLines);

		return codeLines;
	}

	private static List<String> sanitizeBLock(List<String> lines) {
		return lines.stream()
			.filter(line -> !isBlockStart(line))
			.filter(line -> !isBlockEnd(line))
			.collect(Collectors.toList());
	}

	private static List<String> alignLeft(List<String> lines) {
		int minTabCount = Integer.MAX_VALUE;
		for (String line : lines) {
			String trimmedLine = line.trim();
			if (!trimmedLine.isEmpty()) {
				minTabCount = Math.min(minTabCount, line.indexOf(trimmedLine));
			}
		}
		if (minTabCount == Integer.MAX_VALUE) {
			minTabCount = 0;
		}
		final int finalMinTabCount = minTabCount;

		return lines.stream()
			.map(line -> line.length() > finalMinTabCount ? line.substring(finalMinTabCount) : line)
			.collect(Collectors.toList());
	}

	private static List<Block> mergeBlocks(List<Block> blocks) {
		final Map<Optional<String>, List<Block>> blocksByName = blocks.stream().collect(Collectors.groupingBy(Block::getName));
		final List<Block> mergedBlocks = new ArrayList<>();
		blocksByName.forEach((key, value) -> {
			List<String> mergedLines = new ArrayList<>();
			for (int i = 0; i < value.size(); i++) {
				Block block = value.get(i);
				mergedLines.addAll(block.getLines());
				if (i < value.size() - 1) {
					mergedLines.add("");
				}
			}
			mergedBlocks.add(new Block(key.orElse(null), mergedLines));
		});
		return mergedBlocks;
	}

	private static void write(String fileName, Block block, Language language, Path outputDirectory) {
		String nameInfix = block.getName().map(name -> "_" + toSnakeCase(name)).orElse("");
		Path outputFile = outputDirectory.resolve(String.format("%s%s.md", toSnakeCase(fileName), nameInfix));

		List<String> lines = concat(
			Stream.of(String.format("{%% highlight %s %%}", language.getMarkdownLanguageIdentifier())),
			block.getLines().stream(),
			Stream.of("{% endhighlight %}"))
			.collect(Collectors.toList());

		if (LOG.isDebugEnabled()) {
			LOG.info("Writing file {}:{}{}", outputDirectory.getParent().getParent().relativize(outputFile), System.lineSeparator(), lines.stream().collect(Collectors.joining(System.lineSeparator())));
		} else {
			LOG.info("Writing file '{}'.", outputDirectory.getParent().getParent().relativize(outputFile));
		}

		try {
			Files.createDirectories(outputFile.getParent());
			try (BufferedWriter bufferedWriter = Files.newBufferedWriter(outputFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
				for (int i = 0; i < lines.size(); i++) {
					bufferedWriter.write(lines.get(i));
					if (i < lines.size() - 1) {
						bufferedWriter.write('\n');
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to write file.", e);
		}
	}

	private static String toSnakeCase(String s) {
		return s.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
	}

	private static boolean isBlockStart(String line) {
		boolean matches = START_COMMENT_PATTERN.matcher(line).matches();
		return matches;
	}

	private static boolean isBlockEnd(String line) {
		return line.matches("\t*// >.*");
	}

	private static List<Directive> parseDirective(String line) {
		List<Directive> directives = new ArrayList<>();
		if (DIRECTIVE_COMMENT_PATTERN.matcher(line).matches()) {
			Matcher directiveExtractor = DIRECTIVE_PATTERN.matcher(line);
			while (directiveExtractor.find()) {
				String directive = directiveExtractor.group("directive");
				String arg = directiveExtractor.group("arg");
				for (DirectiveFactory directiveFactory : DirectiveFactory.values()) {
					if (directiveFactory.keyword.equals(directive)) {
						directives.add(directiveFactory.get(arg));
					}
				}
			}
		}
		return directives;
	}

	private static List<String> processDirectives(List<String> lines) {
		List<String> processedLines = new ArrayList<>();
		for (int i = 0; i < lines.size(); i++) {
			String line = lines.get(i);
			List<Directive> directiveFactories = parseDirective(line);
			if (directiveFactories.size() == 0) {
				processedLines.add(line);
			} else {
				i++;
				String base = lines.get(i);
				while (lines.get(i + 1).trim().startsWith(".")) {
					base += lines.get(i + 1).trim();
					i++;
				}
				for (Directive directive : directiveFactories) {
					base = directive.apply(base);
				}
				processedLines.add(base);
			}
		}
		return processedLines;
	}

	private interface Directive {
		String apply(String line);
	}

	private enum DirectiveFactory {
		HIDE_ASSIGNMENT("hide-assignment", (line, arg) -> line.replaceFirst("(.*) = .*?;", "$1 = ...")),
		HIDE_ARGUMENT("hide-argument", (line, arg) -> line.replaceFirst("(.*)\\(.*\\);", "$1(" + argOrDots(arg) + ");"));

		private final String keyword;
		private final BiFunction<String, String, String> processor;

		DirectiveFactory(String keyword, BiFunction<String, String, String> processor) {
			this.keyword = keyword;
			this.processor = processor;
		}

		public Directive get(@Nullable String arg) {
			return line -> processor.apply(line, arg);
		}

		private static String argOrDots(@Nullable String arg) {
			return arg == null
				? "..."
				: "/* " + arg + " */";
		}
	}

	private static class Block {

		private final Optional<String> name;
		private final List<String> lines;

		private Block(List<String> lines) {
			this(null, lines);
		}

		private Block(@Nullable String name, List<String> lines) {
			this.name = name == null || name.isEmpty() ? Optional.empty() : Optional.of(name);
			this.lines = Preconditions.checkNotNull(lines);
		}

		public Optional<String> getName() {
			return name;
		}

		public List<String> getLines() {
			return lines;
		}
	}

	private enum Language {

		JAVA(
			"java",
			path -> path.getFileName().toString().endsWith(".java"),
			Paths.get("src", "main", "java", "org", "apache", "flink", "docs", "examples")),
		SCALA(
			"scala",
			path -> path.getFileName().toString().endsWith(".scala"),
			Paths.get("src", "main", "scala", "org", "apache", "flink", "docs", "examples"));
		//Python("python")

		private final String markdownLanguageIdentifier;
		private final Predicate<Path> exampleFileFilter;
		private final Path relativeExampleDirectory;

		Language(String markdownLanguageIdentifier, Predicate<Path> exampleFileFilter, Path relativeExampleDirectory) {
			this.markdownLanguageIdentifier = markdownLanguageIdentifier;
			this.exampleFileFilter = exampleFileFilter;
			this.relativeExampleDirectory = relativeExampleDirectory;
		}

		public String getMarkdownLanguageIdentifier() {
			return markdownLanguageIdentifier;
		}

		public Predicate<Path> getExampleFileFilter() {
			return exampleFileFilter;
		}

		public Path getRelativeExampleDirectory() {
			return relativeExampleDirectory;
		}
	}

	private static <X> Stream<X> concat(Stream<X>... streams) {
		Stream<X> base = streams[0];
		for (int i = 1; i < streams.length; i++) {
			base = Stream.concat(base, streams[i]);
		}
		return base;
	}
}
