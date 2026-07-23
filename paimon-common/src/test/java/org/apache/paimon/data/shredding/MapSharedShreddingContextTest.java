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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MapSharedShreddingContext}. */
class MapSharedShreddingContextTest {

    @Test
    void testFirstFileUsesKMax() {
        MapSharedShreddingContext context = context("tags", 256);

        assertThat(context.computeNextK()).containsEntry("tags", 256);
    }

    @Test
    void testAdaptKAfterOneFile() {
        MapSharedShreddingContext context = context("tags", 256, 7);

        assertThat(context.computeNextK()).containsEntry("tags", 7);
    }

    @Test
    void testAdaptKCappedByKMax() {
        MapSharedShreddingContext context = context("tags", 10, 20);

        assertThat(context.computeNextK()).containsEntry("tags", 10);
    }

    @Test
    void testHistoryP90UsesMaxWhenSamplesAreClose() {
        MapSharedShreddingContext context = context("tags", 256, 3, 7, 5);

        assertThat(context.computeNextK()).containsEntry("tags", 7);
    }

    @Test
    void testHistoryP90IgnoresSingleFarOutlier() {
        List<Integer> widths = new ArrayList<>();
        for (int i = 0; i < 19; i++) {
            widths.add(3);
        }
        widths.add(1000);
        MapSharedShreddingContext context = context("tags", 256, widths);

        assertThat(context.computeNextK()).containsEntry("tags", 3);
    }

    @Test
    void testHistoryP90UsesMaxWithinAbsoluteSlack() {
        List<Integer> widths = new ArrayList<>();
        for (int i = 0; i < 19; i++) {
            widths.add(3);
        }
        widths.add(7);
        MapSharedShreddingContext context = context("tags", 256, widths);

        assertThat(context.computeNextK()).containsEntry("tags", 7);
    }

    @Test
    void testHistoryP90UsesMaxWithinRelativeSlack() {
        List<Integer> widths = new ArrayList<>();
        for (int i = 0; i < 19; i++) {
            widths.add(100);
        }
        widths.add(125);
        MapSharedShreddingContext context = context("tags", 256, widths);

        assertThat(context.computeNextK()).containsEntry("tags", 125);
    }

    @Test
    void testHistoryP90IgnoresMaxBeyondBothSlacks() {
        List<Integer> widths = new ArrayList<>();
        for (int i = 0; i < 19; i++) {
            widths.add(100);
        }
        widths.add(130);
        MapSharedShreddingContext context = context("tags", 256, widths);

        assertThat(context.computeNextK()).containsEntry("tags", 100);
    }

    @Test
    void testMultipleColumnsIndependent() {
        Map<String, Integer> columns = columns("tags", 256);
        columns.put("attrs", 128);
        Map<String, List<Integer>> widths = new LinkedHashMap<>();
        widths.put("tags", Collections.singletonList(10));
        widths.put("attrs", Collections.singletonList(5));
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns, widths);

        assertThat(context.computeNextK()).containsEntry("tags", 10).containsEntry("attrs", 5);
    }

    @Test
    void testMultipleColumnsUseIndependentHistoryWindows() {
        Map<String, Integer> columns = columns("tags", 256);
        columns.put("attrs", 128);
        columns.put("missing", 64);

        List<Integer> tagWidths = new ArrayList<>();
        List<Integer> attrWidths = new ArrayList<>();
        for (int i = 0; i < 19; i++) {
            tagWidths.add(3);
            attrWidths.add(100);
        }
        tagWidths.add(1000);
        attrWidths.add(125);

        Map<String, List<Integer>> widths = new LinkedHashMap<>();
        widths.put("tags", tagWidths);
        widths.put("attrs", attrWidths);
        MapSharedShreddingContext context = new MapSharedShreddingContext(columns, widths);

        assertThat(context.computeNextK())
                .containsEntry("tags", 3)
                .containsEntry("attrs", 125)
                .containsEntry("missing", 64);
    }

    private static MapSharedShreddingContext context(
            String name, int maxColumns, int... historicalWidths) {
        List<Integer> widths = new ArrayList<>();
        for (int width : historicalWidths) {
            widths.add(width);
        }
        return context(name, maxColumns, widths);
    }

    private static MapSharedShreddingContext context(
            String name, int maxColumns, List<Integer> historicalWidths) {
        Map<String, List<Integer>> widths = new LinkedHashMap<>();
        widths.put(name, historicalWidths);
        return new MapSharedShreddingContext(columns(name, maxColumns), widths);
    }

    private static Map<String, Integer> columns(String name, int maxColumns) {
        Map<String, Integer> columns = new LinkedHashMap<>();
        columns.put(name, maxColumns);
        return columns;
    }
}
