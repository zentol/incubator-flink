package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.MetricScope;
import org.apache.flink.runtime.metrics.MetricRegistry;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Default scope implementation. Contains additional methods assembling identifiers based on reporter-specific delimiters.
 */
@Internal
public class InternalMetricScope implements MetricScope {

	private final MetricRegistry registry;
	private final Supplier<Map<String, String>> variablesProvider;

	/**
	 * The map containing all variables and their associated values, lazily computed.
	 */
	protected volatile Map<String, String> variables;

	/**
	 * The metrics scope represented by this group.
	 * For example ["host-7", "taskmanager-2", "window_word_count", "my-mapper" ].
	 */
	private final String[] scopeComponents;

	/**
	 * Array containing the metrics scope represented by this group for each reporter, as a concatenated string, lazily computed.
	 * For example: "host-7.taskmanager-2.window_word_count.my-mapper"
	 */
	private final String[] scopeStrings;

	/**
	 * The metrics scope represented by this group.
	 * For example ["host-7", "taskmanager-2", "window_word_count", "my-mapper" ].
	 */
	private final String[] logicalScopeComponents;

	/** The logical metrics scope represented by this group for each reporter, as a concatenated string, lazily computed.
	 * For example: "taskmanager.job.task" */
	private String[] logicalScopeStrings;

	public InternalMetricScope(MetricRegistry registry, String[] scopeComponents, String[] logicalScopeStrings, Supplier<Map<String, String>> variablesProvider) {
		this.registry = registry;
		this.variablesProvider = variablesProvider;
		this.scopeComponents = scopeComponents;
		this.scopeStrings = new String[registry.getNumberReporters()];
		this.logicalScopeComponents = logicalScopeStrings;
		this.logicalScopeStrings = new String[registry.getNumberReporters()];
	}

	@Override
	public Map<String, String> getAllVariables() {
		if (variables == null) { // avoid synchronization for common case
			synchronized (this) {
				if (variables == null) {
					variables = variablesProvider.get();
				}
			}
		}
		return variables;
	}

	public String[] geScopeComponents() {
		return scopeStrings;
	}

	public String[] getLogicalScopeComponents() {
		return logicalScopeStrings;
	}

	@Override
	public String getMetricIdentifier(String metricName) {
		return getMetricIdentifier(metricName, s -> s, registry.getDelimiter(), -1);
	}

	@Override
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		return getMetricIdentifier(metricName, filter, registry.getDelimiter(), -1);
	}

	@Internal
	public String getMetricIdentifier(String metricName, CharacterFilter filter, int reporterIndex) {
		return getMetricIdentifier(metricName, filter, registry.getDelimiter(reporterIndex), reporterIndex);
	}

	private String getMetricIdentifier(String metricName, CharacterFilter filter, char delimiter, int reporterIndex) {
		return getIdentifierScope(filter, delimiter, reporterIndex) + delimiter + filter.filterCharacters(metricName);
	}

	private String getIdentifierScope(CharacterFilter filter, char delimiter, int reporterIndex) {
		if (scopeStrings.length == 0 || (reporterIndex < 0 || reporterIndex >= scopeStrings.length)) {
			return ScopeFormat.concat(filter, delimiter, scopeComponents);
		} else {
			if (scopeStrings[reporterIndex] == null) {
				scopeStrings[reporterIndex] = ScopeFormat.concat(filter, delimiter, scopeComponents);
			}
			return scopeStrings[reporterIndex];
		}
	}

	@Override
	public String getLogicalMetricIdentifier(String metricName) {
		return getLogicalMetricIdentifier(metricName, s -> s, registry.getDelimiter(), -1);
	}

	@Override
	public String getLogicalMetricIdentifier(String metricName, CharacterFilter filter) {
		return getLogicalMetricIdentifier(metricName, filter, registry.getDelimiter(), -1);
	}

	@Internal
	public String getLogicalMetricIdentifier(String metricName, CharacterFilter filter, int reporterIndex) {
		return getLogicalMetricIdentifier(metricName, filter, registry.getDelimiter(reporterIndex), reporterIndex);
	}

	@Internal
	public String getLogicalMetricIdentifier(String metricName, CharacterFilter filter, char delimiter, int reporterIndex) {
		return getLogicalIdentifierScope(filter, delimiter, reporterIndex) + delimiter + filter.filterCharacters(metricName);
	}

	/**
	 * Returns the logical scope of this group, for example
	 * {@code "taskmanager.job.task"}.
	 *
	 * @param filter character filter which is applied to the scope components
	 * @param delimiter delimiter to use for concatenating scope components
	 * @param reporterIndex index of the reporter
	 * @return logical scope
	 */
	private String getLogicalIdentifierScope(CharacterFilter filter, char delimiter, int reporterIndex) {
		if (logicalScopeStrings.length == 0 || (reporterIndex < 0 || reporterIndex >= logicalScopeStrings.length)) {
			return ScopeFormat.concat(filter, delimiter, logicalScopeComponents);
		} else {
			if (logicalScopeStrings[reporterIndex] == null) {
				logicalScopeStrings[reporterIndex] = ScopeFormat.concat(filter, delimiter, logicalScopeComponents);
			}
			return logicalScopeStrings[reporterIndex];
		}
	}
}
