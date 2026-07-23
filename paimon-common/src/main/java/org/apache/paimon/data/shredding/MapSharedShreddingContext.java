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

package org.apache.paimon.data.shredding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Inputs used to choose physical column counts for one shared-shredding MAP file. */
public class MapSharedShreddingContext {

    private static final double PERCENTILE_RATIO = 0.90;
    private static final int MAX_CLOSE_ABSOLUTE_SLACK = 4;
    private static final double MAX_CLOSE_RELATIVE_RATIO = 1.25;

    private final Map<String, Integer> fieldToMaxColumns;
    private final Map<String, List<Integer>> fieldToHistoricalMaxRowWidths;

    public MapSharedShreddingContext(
            Map<String, Integer> fieldToMaxColumns,
            Map<String, List<Integer>> fieldToHistoricalMaxRowWidths) {
        this.fieldToMaxColumns = new TreeMap<>(fieldToMaxColumns);
        this.fieldToHistoricalMaxRowWidths = new TreeMap<>(fieldToHistoricalMaxRowWidths);
    }

    /** Returns the physical column count K to use for each shared-shredding field. */
    public Map<String, Integer> computeNextK() {
        Map<String, Integer> result = new TreeMap<>();
        for (Map.Entry<String, Integer> entry : fieldToMaxColumns.entrySet()) {
            String fieldName = entry.getKey();
            int maxColumns = entry.getValue();
            List<Integer> widths = fieldToHistoricalMaxRowWidths.get(fieldName);
            if (widths == null || widths.isEmpty()) {
                result.put(fieldName, maxColumns);
            } else {
                int adaptiveWidth = computeAdaptiveWidth(new ArrayList<>(widths));
                result.put(fieldName, Math.max(1, Math.min(adaptiveWidth, maxColumns)));
            }
        }
        return result;
    }

    private static int computeAdaptiveWidth(List<Integer> values) {
        checkArgument(!values.isEmpty(), "values should not be empty.");

        Collections.sort(values);
        int maxWidth = values.get(values.size() - 1);
        int percentileRank = (int) Math.ceil(PERCENTILE_RATIO * values.size());
        percentileRank = Math.max(1, Math.min(percentileRank, values.size()));
        int percentileWidth = values.get(percentileRank - 1);

        int relativeCloseThreshold =
                (int) Math.ceil((double) percentileWidth * MAX_CLOSE_RELATIVE_RATIO);
        if (maxWidth - percentileWidth <= MAX_CLOSE_ABSOLUTE_SLACK
                || maxWidth <= relativeCloseThreshold) {
            return maxWidth;
        }
        return percentileWidth;
    }
}
