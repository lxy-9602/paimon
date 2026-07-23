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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.data.variant.InferVariantShreddingSchema;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.data.variant.VariantShreddingWritePlan;
import org.apache.paimon.format.shredding.ShreddingFileMetadata;
import org.apache.paimon.format.shredding.ShreddingWritePlanFactory;
import org.apache.paimon.format.shredding.ShreddingWritePlanHistory;
import org.apache.paimon.format.shredding.ShreddingWritePlanPreparation;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.utils.JsonSerdeUtil;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Creates Variant shredding write plans from configured or inferred schemas. */
public class VariantShreddingWritePlanFactory implements ShreddingWritePlanFactory {

    private final RowType rowType;
    private final Options options;

    public VariantShreddingWritePlanFactory(RowType rowType, Options options) {
        this.rowType = rowType;
        this.options = options;
    }

    @Override
    public RowType logicalRowType() {
        return rowType;
    }

    @Override
    public boolean shouldCreateWritePlan() {
        return hasConfiguredShreddingSchema() || shouldInferFromRows();
    }

    private boolean shouldInferFromRows() {
        if (hasConfiguredShreddingSchema()) {
            return false;
        }

        if (!options.get(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA)) {
            return false;
        }

        return containsVariantFields(rowType);
    }

    @Override
    public boolean requiresHistory() {
        return shouldInferFromRows()
                && options.get(CoreOptions.VARIANT_RESTORE_SHREDDING_SCHEMA_FROM_HISTORY);
    }

    @Override
    public ShreddingWritePlanPreparation prepare(ShreddingWritePlanHistory history) {
        checkArgument(shouldCreateWritePlan(), "Variant shredding write plan is not active.");
        if (hasConfiguredShreddingSchema()) {
            return ShreddingWritePlanPreparation.ready(
                    VariantShreddingWritePlan.fromConfiguredSchema(
                            rowType, configuredShreddingSchema()));
        }

        if (!options.get(CoreOptions.VARIANT_RESTORE_SHREDDING_SCHEMA_FROM_HISTORY)) {
            return ShreddingWritePlanPreparation.sampleRows(
                    rowType,
                    options.get(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW),
                    sampleRows ->
                            new VariantShreddingWritePlan(
                                    rowType, createInferrer().inferSchema(sampleRows)));
        }

        RecoveredSchema recoveredSchema = recoverPhysicalSchema(history);
        if (recoveredSchema.fullyRecovered) {
            return ShreddingWritePlanPreparation.ready(
                    new VariantShreddingWritePlan(rowType, recoveredSchema.physicalRowType));
        }

        return ShreddingWritePlanPreparation.sampleRows(
                rowType,
                options.get(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW),
                sampleRows -> createInferredWritePlan(sampleRows, recoveredSchema.physicalRowType));
    }

    private ShreddingWritePlan createInferredWritePlan(
            List<InternalRow> sampleRows, RowType recoveredPhysicalRowType) {
        RowType inferredPhysicalRowType = createInferrer().inferSchema(sampleRows);
        RowType physicalRowType =
                mergeRecoveredSchema(rowType, recoveredPhysicalRowType, inferredPhysicalRowType);
        return new VariantShreddingWritePlan(rowType, physicalRowType);
    }

    private RecoveredSchema recoverPhysicalSchema(ShreddingWritePlanHistory history) {
        List<RowType> historicalRowTypes = new ArrayList<>();
        List<ShreddingFileMetadata> files = history.files();
        for (int i = files.size() - 1; i >= 0; i--) {
            RowType physicalRowType = files.get(i).physicalRowType();
            if (physicalRowType != null) {
                historicalRowTypes.add(physicalRowType);
            }
        }
        return recoverPhysicalSchema(rowType, historicalRowTypes);
    }

    private RecoveredSchema recoverPhysicalSchema(
            RowType logicalType, List<RowType> historicalRowTypes) {
        List<DataField> physicalFields = new ArrayList<>();
        boolean fullyRecovered = true;
        for (DataField logicalField : logicalType.getFields()) {
            List<DataType> historicalTypes = findHistoricalTypes(logicalField, historicalRowTypes);
            DataType logicalFieldType = logicalField.type();
            if (logicalFieldType instanceof VariantType) {
                RowType recoveredVariantType = firstVariantPhysicalType(historicalTypes);
                if (recoveredVariantType == null) {
                    physicalFields.add(logicalField);
                    fullyRecovered = false;
                } else {
                    physicalFields.add(logicalField.newType(recoveredVariantType));
                }
            } else if (logicalFieldType instanceof RowType) {
                List<RowType> historicalNestedTypes = new ArrayList<>();
                for (DataType historicalType : historicalTypes) {
                    if (historicalType instanceof RowType) {
                        historicalNestedTypes.add((RowType) historicalType);
                    }
                }
                RecoveredSchema nested =
                        recoverPhysicalSchema((RowType) logicalFieldType, historicalNestedTypes);
                physicalFields.add(logicalField.newType(nested.physicalRowType));
                fullyRecovered &= nested.fullyRecovered;
            } else {
                physicalFields.add(logicalField);
            }
        }
        return new RecoveredSchema(
                new RowType(logicalType.isNullable(), physicalFields), fullyRecovered);
    }

    private List<DataType> findHistoricalTypes(
            DataField logicalField, List<RowType> historicalRowTypes) {
        List<DataType> result = new ArrayList<>();
        for (RowType historicalRowType : historicalRowTypes) {
            DataField historicalField = findHistoricalField(logicalField, historicalRowType);
            if (historicalField != null) {
                result.add(historicalField.type());
            }
        }
        return result;
    }

    @Nullable
    private DataField findHistoricalField(DataField logicalField, RowType historicalRowType) {
        for (DataField historicalField : historicalRowType.getFields()) {
            if (historicalField.id() == logicalField.id()) {
                return historicalField;
            }
        }
        return null;
    }

    @Nullable
    private RowType firstVariantPhysicalType(List<DataType> historicalTypes) {
        for (DataType historicalType : historicalTypes) {
            if (!(historicalType instanceof RowType)) {
                continue;
            }
            RowType historicalRowType = (RowType) historicalType;
            try {
                PaimonShreddingUtils.buildVariantSchema(historicalRowType);
                return historicalRowType;
            } catch (RuntimeException e) {
                // Continue with an older physical schema.
            }
        }
        return null;
    }

    private RowType mergeRecoveredSchema(
            RowType logicalType, RowType recoveredType, RowType inferredType) {
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < logicalType.getFieldCount(); i++) {
            DataField logicalField = logicalType.getFields().get(i);
            DataType logicalFieldType = logicalField.type();
            DataType recoveredFieldType = recoveredType.getTypeAt(i);
            DataType inferredFieldType = inferredType.getTypeAt(i);
            if (logicalFieldType instanceof VariantType) {
                fields.add(
                        logicalField.newType(
                                recoveredFieldType instanceof RowType
                                        ? recoveredFieldType
                                        : inferredFieldType));
            } else if (logicalFieldType instanceof RowType) {
                fields.add(
                        logicalField.newType(
                                mergeRecoveredSchema(
                                        (RowType) logicalFieldType,
                                        (RowType) recoveredFieldType,
                                        (RowType) inferredFieldType)));
            } else {
                fields.add(logicalField);
            }
        }
        return new RowType(logicalType.isNullable(), fields);
    }

    private boolean hasConfiguredShreddingSchema() {
        return options.contains(CoreOptions.VARIANT_SHREDDING_SCHEMA);
    }

    private RowType configuredShreddingSchema() {
        String shreddingSchema = options.get(CoreOptions.VARIANT_SHREDDING_SCHEMA);
        return (RowType) JsonSerdeUtil.fromJson(shreddingSchema, DataType.class);
    }

    private InferVariantShreddingSchema createInferrer() {
        return new InferVariantShreddingSchema(
                rowType,
                options.get(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH),
                options.get(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_DEPTH),
                options.get(CoreOptions.VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO));
    }

    private boolean containsVariantFields(RowType rowType) {
        for (DataField field : rowType.getFields()) {
            if (field.type() instanceof VariantType) {
                return true;
            }
            if (field.type() instanceof RowType && containsVariantFields((RowType) field.type())) {
                return true;
            }
        }
        return false;
    }

    private static class RecoveredSchema {

        private final RowType physicalRowType;
        private final boolean fullyRecovered;

        private RecoveredSchema(RowType physicalRowType, boolean fullyRecovered) {
            this.physicalRowType = physicalRowType;
            this.fullyRecovered = fullyRecovered;
        }
    }
}
