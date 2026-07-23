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

import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Format-independent schema and field metadata recovered from one data file. */
public class ShreddingFileMetadata {

    @Nullable private final RowType physicalRowType;
    private final Map<String, Map<String, String>> fieldMetadata;

    public ShreddingFileMetadata(
            @Nullable RowType physicalRowType, Map<String, Map<String, String>> fieldMetadata) {
        this.physicalRowType = physicalRowType;
        this.fieldMetadata = immutableFieldMetadata(fieldMetadata);
    }

    @Nullable
    public RowType physicalRowType() {
        return physicalRowType;
    }

    public Map<String, Map<String, String>> fieldMetadata() {
        return fieldMetadata;
    }

    private static Map<String, Map<String, String>> immutableFieldMetadata(
            Map<String, Map<String, String>> fieldMetadata) {
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : fieldMetadata.entrySet()) {
            result.put(
                    entry.getKey(),
                    Collections.unmodifiableMap(new LinkedHashMap<>(entry.getValue())));
        }
        return Collections.unmodifiableMap(result);
    }
}
