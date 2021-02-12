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

package org.apache.flink.configuration.description;

import java.util.EnumSet;

/** Formatter that transforms {@link Description} into Html representation. */
public class MarkdownFormatter extends Formatter {

    @Override
    protected void formatLink(StringBuilder state, String link, String description) {
        state.append(String.format("[%s](%s)", description, link));
    }

    @Override
    protected void formatLineBreak(StringBuilder state) {
        state.append("<br />");
    }

    @Override
    protected void formatText(
            StringBuilder state,
            String format,
            String[] elements,
            EnumSet<TextElement.TextStyle> styles) {

        String prefix = "";
        String suffix = "";
        if (styles.contains(TextElement.TextStyle.CODE)) {
            prefix = "`";
            suffix = "`";
        }
        state.append(prefix);
        state.append(String.format(format, elements));
        state.append(suffix);
    }

    @Override
    protected void formatList(StringBuilder state, String[] entries) {
        state.append("<ul>");
        for (String entry : entries) {
            state.append(String.format("<li>%s</li>", entry));
        }
        state.append("</ul>");
    }

    @Override
    protected Formatter newInstance() {
        return new MarkdownFormatter();
    }
}
