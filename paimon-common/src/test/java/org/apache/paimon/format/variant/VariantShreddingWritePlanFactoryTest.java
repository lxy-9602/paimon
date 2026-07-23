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

package org.apache.paimon.format.variant;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.format.shredding.ShreddingFileMetadata;
import org.apache.paimon.format.shredding.ShreddingWritePlanHistory;
import org.apache.paimon.format.shredding.ShreddingWritePlanPreparation;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.paimon.format.shredding.ShreddingWritePlanPreparationTestUtils.createPlan;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VariantShreddingWritePlanFactory}. */
class VariantShreddingWritePlanFactoryTest {

    @Test
    void testRestorePhysicalSchemaFromHistoryByFieldId() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "renamed_v", DataTypes.VARIANT()));
        RowType historicalVariantType =
                PaimonShreddingUtils.variantShreddingSchema(
                        DataTypes.ROW(DataTypes.FIELD(0, "age", DataTypes.BIGINT())));
        RowType historicalPhysicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "v", historicalVariantType));

        ShreddingWritePlanPreparation preparation =
                createFactory(logicalType, true).prepare(history(historicalPhysicalType));

        ShreddingWritePlan writePlan = createPlan(preparation, Collections.emptyList());
        assertThat(writePlan.physicalRowType().getTypeAt(1)).isEqualTo(historicalVariantType);
        assertThat(writePlan.physicalRowType().getFieldNames()).containsExactly("id", "renamed_v");
    }

    @Test
    void testInferOnlyVariantFieldsMissingFromHistory() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "v", DataTypes.VARIANT()),
                        DataTypes.FIELD(2, "new_v", DataTypes.VARIANT()));
        RowType historicalVariantType =
                PaimonShreddingUtils.variantShreddingSchema(
                        DataTypes.ROW(DataTypes.FIELD(0, "age", DataTypes.BIGINT())));
        RowType historicalPhysicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "v", historicalVariantType));

        ShreddingWritePlanPreparation preparation =
                createFactory(logicalType, true).prepare(history(historicalPhysicalType));
        ShreddingWritePlan writePlan =
                createPlan(
                        preparation,
                        Collections.singletonList(
                                GenericRow.of(
                                        1,
                                        GenericVariant.fromJson("{\"other\":1}"),
                                        GenericVariant.fromJson("{\"name\":\"Alice\"}"))));
        assertThat(writePlan.physicalRowType().getTypeAt(1)).isEqualTo(historicalVariantType);

        RowType newVariantType = (RowType) writePlan.physicalRowType().getTypeAt(2);
        RowType typedValueType =
                (RowType)
                        newVariantType.getField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME).type();
        assertThat(typedValueType.getFieldNames()).containsExactly("name");
    }

    @Test
    void testInferenceIgnoresHistoryByDefault() {
        RowType logicalType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));
        RowType historicalVariantType =
                PaimonShreddingUtils.variantShreddingSchema(
                        DataTypes.ROW(DataTypes.FIELD(0, "age", DataTypes.BIGINT())));
        RowType historicalPhysicalType =
                DataTypes.ROW(DataTypes.FIELD(0, "v", historicalVariantType));

        ShreddingWritePlanPreparation preparation =
                createFactory(logicalType, false).prepare(history(historicalPhysicalType));
        ShreddingWritePlan writePlan =
                createPlan(
                        preparation,
                        Collections.singletonList(
                                GenericRow.of(GenericVariant.fromJson("{\"name\":\"Alice\"}"))));

        RowType variantType = (RowType) writePlan.physicalRowType().getTypeAt(0);
        RowType typedValueType =
                (RowType) variantType.getField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME).type();
        assertThat(typedValueType.getFieldNames()).containsExactly("name");
    }

    @Test
    void testRestoreAndInferVariantsInNestedRow() {
        RowType logicalNestedType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "restored_v", DataTypes.VARIANT()),
                        DataTypes.FIELD(3, "inferred_v", DataTypes.VARIANT()));
        RowType logicalType = DataTypes.ROW(DataTypes.FIELD(0, "nested", logicalNestedType));
        RowType historicalVariantType =
                PaimonShreddingUtils.variantShreddingSchema(
                        DataTypes.ROW(DataTypes.FIELD(0, "age", DataTypes.BIGINT())));
        RowType historicalNestedType =
                DataTypes.ROW(
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "restored_v", historicalVariantType));
        RowType historicalPhysicalType =
                DataTypes.ROW(DataTypes.FIELD(0, "nested", historicalNestedType));

        ShreddingWritePlanPreparation preparation =
                createFactory(logicalType, true).prepare(history(historicalPhysicalType));
        ShreddingWritePlan writePlan =
                createPlan(
                        preparation,
                        Collections.singletonList(
                                GenericRow.of(
                                        GenericRow.of(
                                                1,
                                                GenericVariant.fromJson("{\"other\":1}"),
                                                GenericVariant.fromJson("{\"name\":\"Alice\"}")))));

        RowType physicalNestedType = (RowType) writePlan.physicalRowType().getTypeAt(0);
        assertThat(physicalNestedType.getTypeAt(0)).isEqualTo(DataTypes.INT());
        assertThat(physicalNestedType.getTypeAt(1)).isEqualTo(historicalVariantType);
        RowType inferredVariantType = (RowType) physicalNestedType.getTypeAt(2);
        RowType typedValueType =
                (RowType)
                        inferredVariantType
                                .getField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME)
                                .type();
        assertThat(typedValueType.getFieldNames()).containsExactly("name");
    }

    private static VariantShreddingWritePlanFactory createFactory(
            RowType logicalType, boolean restoreFromHistory) {
        Options options = new Options();
        options.set(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA, true);
        options.set(CoreOptions.VARIANT_RESTORE_SHREDDING_SCHEMA_FROM_HISTORY, restoreFromHistory);
        options.set(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW, 1);
        return new VariantShreddingWritePlanFactory(logicalType, options);
    }

    private static ShreddingWritePlanHistory history(RowType physicalType) {
        return new ShreddingWritePlanHistory(
                Collections.singletonList(
                        new ShreddingFileMetadata(physicalType, Collections.emptyMap())));
    }
}
