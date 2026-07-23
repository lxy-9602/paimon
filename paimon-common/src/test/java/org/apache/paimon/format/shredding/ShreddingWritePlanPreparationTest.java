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

package org.apache.paimon.format.shredding;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for shredding write plan preparation and history. */
class ShreddingWritePlanPreparationTest {

    private static final RowType ROW_TYPE =
            DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));

    @Test
    void testReadyAndSampledPreparation() {
        ShreddingWritePlan plan = new IdentityWritePlan();
        ShreddingWritePlanPreparation ready = ShreddingWritePlanPreparation.ready(plan);

        assertThat(ready.createPlan(Collections.emptyList())).isSameAs(plan);

        ShreddingWritePlanPreparation sampled =
                ShreddingWritePlanPreparation.sampleRows(ROW_TYPE, 2, ignored -> plan);
        assertThat(sampled.createPlan(Collections.singletonList(GenericRow.of(1)))).isSameAs(plan);
    }

    @Test
    void testHistoryCopiesInputAndAppendsCompletedFiles() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("key", "value");
        Map<String, Map<String, String>> fieldMetadata = new LinkedHashMap<>();
        fieldMetadata.put("field", metadata);
        ShreddingFileMetadata file = new ShreddingFileMetadata(ROW_TYPE, fieldMetadata);
        List<ShreddingFileMetadata> files = new ArrayList<>();
        files.add(file);
        ShreddingWritePlanHistory history = new ShreddingWritePlanHistory(files);

        metadata.put("other", "changed");
        fieldMetadata.clear();
        files.clear();

        ShreddingFileMetadata completedFile =
                new ShreddingFileMetadata(ROW_TYPE, Collections.emptyMap());
        history.add(completedFile);

        assertThat(history.files()).containsExactly(file, completedFile);
        assertThat(file.physicalRowType()).isEqualTo(ROW_TYPE);
        assertThat(file.fieldMetadata().get("field")).containsOnlyKeys("key");
    }

    @Test
    void testHistoryKeepsOnlyRecentFiles() {
        List<ShreddingFileMetadata> files = new ArrayList<>();
        for (int i = 0; i < ShreddingWritePlanHistory.MAX_HISTORY_FILES + 5; i++) {
            files.add(fileMetadata(String.valueOf(i)));
        }

        ShreddingWritePlanHistory history = new ShreddingWritePlanHistory(files);
        assertThat(history.files()).hasSize(ShreddingWritePlanHistory.MAX_HISTORY_FILES);
        assertThat(history.files().get(0).physicalRowType().getFieldNames()).containsExactly("5");

        history.add(fileMetadata("next"));
        assertThat(history.files()).hasSize(ShreddingWritePlanHistory.MAX_HISTORY_FILES);
        assertThat(history.files().get(0).physicalRowType().getFieldNames()).containsExactly("6");
        assertThat(
                        history.files()
                                .get(ShreddingWritePlanHistory.MAX_HISTORY_FILES - 1)
                                .physicalRowType()
                                .getFieldNames())
                .containsExactly("next");
    }

    private static ShreddingFileMetadata fileMetadata(String fieldName) {
        return new ShreddingFileMetadata(
                DataTypes.ROW(DataTypes.FIELD(0, fieldName, DataTypes.INT())),
                Collections.emptyMap());
    }

    private static class IdentityWritePlan implements ShreddingWritePlan {

        @Override
        public RowType logicalRowType() {
            return ROW_TYPE;
        }

        @Override
        public RowType physicalRowType() {
            return ROW_TYPE;
        }

        @Override
        public InternalRow toPhysicalRow(InternalRow row) {
            return row;
        }
    }
}
