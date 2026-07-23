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
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsShreddingFileMetadata;
import org.apache.paimon.format.shredding.ShreddingFileMetadata;
import org.apache.paimon.format.shredding.ShreddingWritePlanHistory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ShreddingWritePlanHistoryLoader}. */
class ShreddingWritePlanHistoryLoaderTest {

    private static final RowType PHYSICAL_TYPE =
            DataTypes.ROW(DataTypes.FIELD(0, "physical", DataTypes.INT()));
    private static final RowType LOGICAL_TYPE =
            DataTypes.ROW(DataTypes.FIELD(0, "field", DataTypes.INT()));

    @Test
    void testLoadSupportedFilesAndSkipIoFailures() {
        MetadataFileFormat parquet = new MetadataFileFormat();
        List<DataFileMeta> restoredFiles =
                Arrays.asList(
                        DataFileTestUtils.newFile("good.parquet", 0, 0, 0, 0),
                        DataFileTestUtils.newFile("bad.parquet", 0, 0, 0, 0),
                        DataFileTestUtils.newFile("ignored.avro", 0, 0, 0, 0));

        ShreddingWritePlanHistory history =
                ShreddingWritePlanHistoryLoader.load(
                        LocalFileIO.create(),
                        restoredFiles,
                        format -> "parquet".equals(format) ? parquet : new UnsupportedFileFormat(),
                        file -> new Path("/tmp", file.fileName()),
                        schemaId -> LOGICAL_TYPE,
                        0);

        assertThat(parquet.readCount).isEqualTo(2);
        assertThat(history.files()).hasSize(1);
        assertThat(history.files().get(0).physicalRowType()).isEqualTo(PHYSICAL_TYPE);
        assertThat(history.files().get(0).fieldMetadata())
                .containsEntry("field", Collections.singletonMap("key", "value"));
    }

    @Test
    void testOrdersHistoryBySequenceNumber() {
        OrderingMetadataFileFormat parquet = new OrderingMetadataFileFormat();
        List<DataFileMeta> restoredFiles =
                Arrays.asList(
                        DataFileTestUtils.newFile("new.parquet", 0, 0, 0, 20),
                        DataFileTestUtils.newFile("old.parquet", 0, 0, 0, 10));

        ShreddingWritePlanHistory history =
                ShreddingWritePlanHistoryLoader.load(
                        LocalFileIO.create(),
                        restoredFiles,
                        format -> parquet,
                        file -> new Path("/tmp", file.fileName()),
                        schemaId -> LOGICAL_TYPE,
                        0);

        assertThat(history.files())
                .extracting(metadata -> metadata.physicalRowType().getFieldNames().get(0))
                .containsExactly("old.parquet", "new.parquet");
    }

    @Test
    void testLoadsOnlyRecentHistoryFiles() {
        OrderingMetadataFileFormat parquet = new OrderingMetadataFileFormat();
        List<DataFileMeta> restoredFiles = new ArrayList<>();
        for (int i = 0; i < ShreddingWritePlanHistory.MAX_HISTORY_FILES + 5; i++) {
            restoredFiles.add(DataFileTestUtils.newFile(i + ".parquet", 0, 0, 0, i));
        }

        ShreddingWritePlanHistory history =
                ShreddingWritePlanHistoryLoader.load(
                        LocalFileIO.create(),
                        restoredFiles,
                        format -> parquet,
                        file -> new Path("/tmp", file.fileName()),
                        schemaId -> LOGICAL_TYPE,
                        0);

        assertThat(parquet.readCount).isEqualTo(ShreddingWritePlanHistory.MAX_HISTORY_FILES);
        assertThat(history.files()).hasSize(ShreddingWritePlanHistory.MAX_HISTORY_FILES);
        assertThat(history.files().get(0).physicalRowType().getFieldNames())
                .containsExactly("5.parquet");
        assertThat(
                        history.files()
                                .get(ShreddingWritePlanHistory.MAX_HISTORY_FILES - 1)
                                .physicalRowType()
                                .getFieldNames())
                .containsExactly("24.parquet");
    }

    @Test
    void testNormalizesRenamedFieldsBySchemaId() {
        MetadataFileFormat parquet = new MetadataFileFormat();
        RowType fileRowType = DataTypes.ROW(DataTypes.FIELD(1, "field", DataTypes.INT()));
        RowType currentRowType =
                DataTypes.ROW(DataTypes.FIELD(1, "renamed_field", DataTypes.INT()));

        ShreddingWritePlanHistory history =
                ShreddingWritePlanHistoryLoader.load(
                        LocalFileIO.create(),
                        Collections.singletonList(
                                DataFileTestUtils.newFile("good.parquet", 0, 0, 0, 0)),
                        format -> parquet,
                        file -> new Path("/tmp", file.fileName()),
                        schemaId -> schemaId == 0 ? fileRowType : currentRowType,
                        1);

        assertThat(history.files()).hasSize(1);
        assertThat(history.files().get(0).fieldMetadata())
                .containsOnlyKeys("renamed_field")
                .containsEntry("renamed_field", Collections.singletonMap("key", "value"));
    }

    @Test
    void testDropsMetadataForRemovedFields() {
        MetadataFileFormat parquet = new MetadataFileFormat();

        ShreddingWritePlanHistory history =
                ShreddingWritePlanHistoryLoader.load(
                        LocalFileIO.create(),
                        Collections.singletonList(
                                DataFileTestUtils.newFile("good.parquet", 0, 0, 0, 0)),
                        format -> parquet,
                        file -> new Path("/tmp", file.fileName()),
                        schemaId ->
                                schemaId == 0
                                        ? DataTypes.ROW(
                                                DataTypes.FIELD(1, "field", DataTypes.INT()))
                                        : RowType.of(),
                        1);

        assertThat(history.files()).hasSize(1);
        assertThat(history.files().get(0).fieldMetadata()).isEmpty();
    }

    @Test
    void testDropsMetadataForUnknownFields() {
        MetadataFileFormat parquet = new MetadataFileFormat();

        ShreddingWritePlanHistory history =
                ShreddingWritePlanHistoryLoader.load(
                        LocalFileIO.create(),
                        Collections.singletonList(
                                DataFileTestUtils.newFile("good.parquet", 0, 0, 0, 0)),
                        format -> parquet,
                        file -> new Path("/tmp", file.fileName()),
                        schemaId -> RowType.of(),
                        0);

        assertThat(history.files()).hasSize(1);
        assertThat(history.files().get(0).fieldMetadata()).isEmpty();
    }

    private static class MetadataFileFormat extends UnsupportedFileFormat
            implements SupportsShreddingFileMetadata {

        private int readCount;

        private MetadataFileFormat() {
            super("parquet");
        }

        @Override
        public ShreddingFileMetadata readShreddingFileMetadata(FormatReaderFactory.Context context)
                throws IOException {
            readCount++;
            if (context.filePath().getName().startsWith("bad")) {
                throw new IOException("Expected test failure");
            }
            return new ShreddingFileMetadata(
                    PHYSICAL_TYPE,
                    Collections.singletonMap("field", Collections.singletonMap("key", "value")));
        }
    }

    private static class OrderingMetadataFileFormat extends UnsupportedFileFormat
            implements SupportsShreddingFileMetadata {

        private int readCount;

        private OrderingMetadataFileFormat() {
            super("parquet");
        }

        @Override
        public ShreddingFileMetadata readShreddingFileMetadata(
                FormatReaderFactory.Context context) {
            readCount++;
            return new ShreddingFileMetadata(
                    DataTypes.ROW(
                            DataTypes.FIELD(0, context.filePath().getName(), DataTypes.INT())),
                    Collections.emptyMap());
        }
    }

    private static class UnsupportedFileFormat extends FileFormat {

        private UnsupportedFileFormat() {
            this("avro");
        }

        private UnsupportedFileFormat(String identifier) {
            super(identifier);
        }

        @Override
        public FormatReaderFactory createReaderFactory(
                RowType dataSchemaRowType,
                RowType projectedRowType,
                @Nullable List<Predicate> filters) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FormatWriterFactory createWriterFactory(RowType type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void validateDataFields(RowType rowType) {}
    }
}
