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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/** A ready shredding write plan or a request to create one from sampled rows. */
public final class ShreddingWritePlanPreparation {

    private final RowType logicalRowType;
    private final int sampleRowCount;
    @Nullable private final ShreddingWritePlan readyPlan;
    @Nullable private final Function<List<InternalRow>, ShreddingWritePlan> sampledPlanCreator;

    private ShreddingWritePlanPreparation(
            RowType logicalRowType,
            int sampleRowCount,
            @Nullable ShreddingWritePlan readyPlan,
            @Nullable Function<List<InternalRow>, ShreddingWritePlan> sampledPlanCreator) {
        this.logicalRowType = logicalRowType;
        this.sampleRowCount = sampleRowCount;
        this.readyPlan = readyPlan;
        this.sampledPlanCreator = sampledPlanCreator;
    }

    public static ShreddingWritePlanPreparation ready(ShreddingWritePlan writePlan) {
        return new ShreddingWritePlanPreparation(writePlan.logicalRowType(), 0, writePlan, null);
    }

    public static ShreddingWritePlanPreparation sampleRows(
            RowType logicalRowType,
            int sampleRowCount,
            Function<List<InternalRow>, ShreddingWritePlan> sampledPlanCreator) {
        Preconditions.checkArgument(sampleRowCount > 0, "Sample row count must be positive.");
        return new ShreddingWritePlanPreparation(
                logicalRowType, sampleRowCount, null, sampledPlanCreator);
    }

    FormatWriter createWriter(
            SupportsShreddingWritePlan writerFactory,
            PositionOutputStream out,
            String compression,
            ShreddingWritePlanHistory history)
            throws IOException {
        if (sampledPlanCreator != null) {
            return new InferShreddingWritePlanWriter(
                    writerFactory,
                    logicalRowType,
                    sampleRowCount,
                    this::createPlan,
                    out,
                    compression,
                    history);
        }
        return ShreddingFormatWriter.create(
                writerFactory, out, compression, createPlan(Collections.emptyList()), history);
    }

    ShreddingWritePlan createPlan(List<InternalRow> sampleRows) {
        return readyPlan != null
                ? readyPlan
                : Preconditions.checkNotNull(sampledPlanCreator).apply(sampleRows);
    }
}
