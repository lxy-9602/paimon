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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.MapSharedShreddingColumnPlacementPolicy;
import org.apache.paimon.format.shredding.ShreddingFileMetadata;
import org.apache.paimon.format.shredding.ShreddingWritePlanFactory;
import org.apache.paimon.format.shredding.ShreddingWritePlanHistory;
import org.apache.paimon.format.shredding.ShreddingWritePlanPreparation;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Creates per-file shared-shredding MAP write plans. */
public class MapSharedShreddingWritePlanFactory implements ShreddingWritePlanFactory {

    private final RowType logicalRowType;
    private final Map<String, Integer> fieldToMaxColumns;
    private final Map<String, MapSharedShreddingColumnPlacementPolicy> fieldToColumnPlacementPolicy;

    public MapSharedShreddingWritePlanFactory(RowType logicalRowType, Options options) {
        this.logicalRowType = logicalRowType;
        CoreOptions coreOptions = new CoreOptions(options);
        List<String> shreddingFields =
                MapSharedShreddingUtils.detectShreddingColumns(logicalRowType, coreOptions);
        this.fieldToMaxColumns =
                MapSharedShreddingUtils.buildColumnToNumColumns(shreddingFields, coreOptions);
        this.fieldToColumnPlacementPolicy = new LinkedHashMap<>();
        for (String field : shreddingFields) {
            fieldToColumnPlacementPolicy.put(
                    field, coreOptions.mapSharedShreddingColumnPlacementPolicy(field));
        }
    }

    @Override
    public RowType logicalRowType() {
        return logicalRowType;
    }

    @Override
    public boolean shouldCreateWritePlan() {
        return !fieldToMaxColumns.isEmpty();
    }

    @Override
    public boolean requiresHistory() {
        return true;
    }

    @Override
    public ShreddingWritePlanPreparation prepare(ShreddingWritePlanHistory history) {
        checkArgument(shouldCreateWritePlan(), "MAP shared-shredding write plan is not active.");
        return ShreddingWritePlanPreparation.ready(createWritePlan(history));
    }

    private ShreddingWritePlan createWritePlan(ShreddingWritePlanHistory history) {
        MapSharedShreddingContext context =
                new MapSharedShreddingContext(
                        fieldToMaxColumns, restoreHistoricalMaxRowWidths(history.files()));

        return new MapSharedShreddingWritePlan(
                logicalRowType, context.computeNextK(), fieldToColumnPlacementPolicy);
    }

    private Map<String, List<Integer>> restoreHistoricalMaxRowWidths(
            List<ShreddingFileMetadata> history) {
        Map<String, List<Integer>> result = new TreeMap<>();
        for (ShreddingFileMetadata file : history) {
            for (String fieldName : fieldToMaxColumns.keySet()) {
                Map<String, String> metadata = file.fieldMetadata().get(fieldName);
                if (!MapSharedShreddingUtils.hasShreddingMetadata(metadata)) {
                    continue;
                }
                try {
                    result.computeIfAbsent(fieldName, ignored -> new ArrayList<>())
                            .add(
                                    MapSharedShreddingUtils.deserializeMetadata(metadata)
                                            .maxRowWidth());
                } catch (IllegalArgumentException e) {
                    // Ignore invalid historical metadata.
                }
            }
        }
        return result;
    }
}
