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

import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.util.function.Supplier;

/** Decorates a raw format writer factory with an active per-file shredding write plan. */
public class ShreddingWritePlanWriterFactory implements FormatWriterFactory {

    private final SupportsShreddingWritePlan delegate;
    private final ShreddingWritePlanFactory writePlanFactory;
    private final Supplier<ShreddingWritePlanHistory> historySupplier;

    public ShreddingWritePlanWriterFactory(
            FormatWriterFactory delegate,
            ShreddingWritePlanFactory writePlanFactory,
            Supplier<ShreddingWritePlanHistory> historySupplier) {
        Preconditions.checkArgument(
                writePlanFactory.shouldCreateWritePlan(),
                "Shredding write plan factory must be active.");
        if (!(delegate instanceof SupportsShreddingWritePlan)) {
            throw new UnsupportedOperationException(
                    "Delegate writer factory does not support shredding write plans: "
                            + delegate.getClass().getName());
        }
        this.delegate = (SupportsShreddingWritePlan) delegate;
        this.writePlanFactory = writePlanFactory;
        this.historySupplier = historySupplier;
    }

    @Override
    public FormatWriter create(PositionOutputStream out, String compression) throws IOException {
        ShreddingWritePlanHistory history =
                writePlanFactory.requiresHistory()
                        ? Preconditions.checkNotNull(historySupplier.get())
                        : ShreddingWritePlanHistory.empty();
        return writePlanFactory.prepare(history).createWriter(delegate, out, compression, history);
    }
}
