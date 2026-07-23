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

package org.apache.paimon.io;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.SupportsShreddingFileMetadata;
import org.apache.paimon.format.shredding.ShreddingFileMetadata;
import org.apache.paimon.format.shredding.ShreddingWritePlanHistory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Loads format-level shredding metadata from restored data files. */
public class ShreddingWritePlanHistoryLoader {

    private static final Logger LOG =
            LoggerFactory.getLogger(ShreddingWritePlanHistoryLoader.class);

    private ShreddingWritePlanHistoryLoader() {}

    public static ShreddingWritePlanHistory load(
            FileIO fileIO,
            List<DataFileMeta> restoredFiles,
            Function<String, FileFormat> formatLoader,
            Function<DataFileMeta, Path> pathResolver,
            Function<Long, RowType> schemaLoader,
            long currentSchemaId) {
        Map<String, FileFormat> formats = new HashMap<>();
        List<ShreddingFileMetadata> files = new ArrayList<>();
        RowType currentRowType = schemaLoader.apply(currentSchemaId);
        List<DataFileMeta> sortedFiles = new ArrayList<>(restoredFiles);
        sortedFiles.sort(
                Comparator.comparingLong(DataFileMeta::maxSequenceNumber)
                        .thenComparingLong(DataFileMeta::creationTimeEpochMillis)
                        .reversed());
        for (DataFileMeta restoredFile : sortedFiles) {
            if (files.size() == ShreddingWritePlanHistory.MAX_HISTORY_FILES) {
                break;
            }
            String formatIdentifier = restoredFile.fileFormat();
            FileFormat format = formats.computeIfAbsent(formatIdentifier, formatLoader::apply);
            if (!(format instanceof SupportsShreddingFileMetadata)) {
                continue;
            }

            Path path = pathResolver.apply(restoredFile);
            try {
                ShreddingFileMetadata metadata =
                        ((SupportsShreddingFileMetadata) format)
                                .readShreddingFileMetadata(
                                        new FormatReaderContext(
                                                fileIO, path, restoredFile.fileSize()));
                metadata =
                        normalizeFieldNames(
                                metadata,
                                schemaLoader.apply(restoredFile.schemaId()),
                                currentRowType);
                files.add(metadata);
            } catch (IOException e) {
                LOG.warn(
                        "Failed to read shredding metadata from restored file {}. Skipping it.",
                        path,
                        e);
            }
        }
        Collections.reverse(files);
        return new ShreddingWritePlanHistory(files);
    }

    private static ShreddingFileMetadata normalizeFieldNames(
            ShreddingFileMetadata metadata, RowType fileRowType, RowType currentRowType) {
        Map<String, Map<String, String>> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : metadata.fieldMetadata().entrySet()) {
            String fieldName = entry.getKey();
            if (!fileRowType.containsField(fieldName)) {
                continue;
            }
            int fieldId = fileRowType.getField(fieldName).id();
            if (!currentRowType.containsField(fieldId)) {
                continue;
            }
            normalized.put(currentRowType.getField(fieldId).name(), entry.getValue());
        }
        return new ShreddingFileMetadata(metadata.physicalRowType(), normalized);
    }
}
