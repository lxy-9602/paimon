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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.shredding.MapSharedShreddingFieldMeta;
import org.apache.paimon.data.shredding.MapSharedShreddingUtils;
import org.apache.paimon.data.shredding.MapShreddingDefine;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsFieldMetadata;
import org.apache.paimon.format.SupportsShreddingFileMetadata;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.format.orc.OrcTypeUtil;
import org.apache.paimon.format.parquet.ParquetFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for format integration of {@link ShreddingWritePlanWriterFactory}. */
class ShreddingWritePlanFormatTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testParquetWritesMapSharedShreddingMetadata() throws Exception {
        Options options = mapSharedShreddingOptions();
        FileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        Map<String, Map<String, String>> fieldMetadata =
                writeAndReadFieldMetadata(format, "parquet", "none");

        assertMapSharedShreddingMetadata(fieldMetadata, "none");
        assertThat(fieldMetadata.get("id"))
                .containsEntry(FormatMetadataUtils.PARQUET_FIELD_ID_KEY, "0");
        assertThat(fieldMetadata.get("tags"))
                .containsEntry(FormatMetadataUtils.PARQUET_FIELD_ID_KEY, "1");
    }

    @Test
    void testOrcWritesMapSharedShreddingMetadata() throws Exception {
        Options options = mapSharedShreddingOptions();
        FileFormat format =
                new OrcFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        Map<String, Map<String, String>> fieldMetadata =
                writeAndReadFieldMetadata(format, "orc", "none");

        assertMapSharedShreddingMetadata(fieldMetadata, "none");
        assertThat(fieldMetadata.get("id")).containsEntry(OrcTypeUtil.PAIMON_ORC_FIELD_ID_KEY, "0");
        assertThat(fieldMetadata.get("tags"))
                .containsEntry(OrcTypeUtil.PAIMON_ORC_FIELD_ID_KEY, "1");
    }

    @Test
    void testOrcRejectsVariantShreddingWritePlan() {
        Options options = new Options();
        options.set(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA, true);
        FileFormat format =
                new OrcFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        assertThatThrownBy(() -> format.createWriterFactory(rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("File format 'orc' does not support VARIANT write plans");
    }

    @Test
    void testRejectsMultipleActiveWritePlans() {
        Options options = mapSharedShreddingOptions();
        options.set(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA, true);
        FileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                        DataTypes.FIELD(1, "v", DataTypes.VARIANT()));

        assertThatThrownBy(() -> format.createWriterFactory(rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Composing multiple active shredding write plans is not supported.");
    }

    @Test
    void testLoadsHistoryOnlyForActiveWritePlan() throws Exception {
        FileFormat format =
                new ParquetFileFormat(
                        new FileFormatFactory.FormatContext(
                                mapSharedShreddingOptions(), 1024, 1024));
        AtomicInteger loadCount = new AtomicInteger();
        FormatWriterFactory writerFactory =
                ShreddingWritePlanWriterFactories.createWriterFactory(
                        format,
                        logicalRowType(),
                        () -> {
                            loadCount.incrementAndGet();
                            return ShreddingWritePlanHistory.empty();
                        });
        assertThat(loadCount).hasValue(0);

        FileIO fileIO = LocalFileIO.create();
        Path file = new Path(tempDir.toString(), UUID.randomUUID() + ".parquet");
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = writerFactory.create(out, "none");
        assertThat(loadCount).hasValue(1);
        writer.addElement(GenericRow.of(1, stringKeyMap("a", 10L)));
        writer.close();
        out.close();
    }

    @Test
    void testUpdatesMapSharedShreddingHistoryAfterEachFile() throws Exception {
        Options options = mapSharedShreddingOptions();
        assertUpdatesMapSharedShreddingHistory(
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024)),
                "parquet");
        assertUpdatesMapSharedShreddingHistory(
                new OrcFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024)), "orc");
    }

    @Test
    void testParquetRestoresVariantSchemaFromPreviousFile() throws Exception {
        assertParquetVariantSchemaForSecondFile(true, "age");
    }

    @Test
    void testParquetInfersVariantSchemaForEachFileByDefault() throws Exception {
        assertParquetVariantSchemaForSecondFile(false, "name");
    }

    private void assertParquetVariantSchemaForSecondFile(
            boolean restoreFromHistory, String expectedField) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA, true);
        options.set(CoreOptions.VARIANT_RESTORE_SHREDDING_SCHEMA_FROM_HISTORY, restoreFromHistory);
        options.set(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW, 1);
        FileFormat format =
                new ParquetFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));
        FileIO fileIO = LocalFileIO.create();
        ShreddingWritePlanHistory history = ShreddingWritePlanHistory.empty();
        AtomicInteger historyLoadCount = new AtomicInteger();
        FormatWriterFactory writerFactory =
                ShreddingWritePlanWriterFactories.createWriterFactory(
                        format,
                        rowType,
                        () -> {
                            historyLoadCount.incrementAndGet();
                            return history;
                        });

        Path firstFile = new Path(tempDir.toString(), UUID.randomUUID() + ".parquet");
        write(
                fileIO,
                firstFile,
                writerFactory,
                GenericRow.of(GenericVariant.fromJson("{\"age\":30}")));
        assertThat(history.files()).hasSize(restoreFromHistory ? 1 : 0);

        Path secondFile = new Path(tempDir.toString(), UUID.randomUUID() + ".parquet");
        write(
                fileIO,
                secondFile,
                writerFactory,
                GenericRow.of(GenericVariant.fromJson("{\"name\":\"Alice\"}")));
        assertThat(history.files()).hasSize(restoreFromHistory ? 2 : 0);
        ShreddingFileMetadata secondMetadata =
                ((SupportsShreddingFileMetadata) format)
                        .readShreddingFileMetadata(
                                new FormatReaderContext(
                                        fileIO, secondFile, fileIO.getFileSize(secondFile)));

        RowType secondVariantType = (RowType) secondMetadata.physicalRowType().getTypeAt(0);
        RowType typedValueType =
                (RowType)
                        secondVariantType
                                .getField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME)
                                .type();
        assertThat(typedValueType.getFieldNames()).containsExactly(expectedField);
        assertThat(historyLoadCount).hasValue(restoreFromHistory ? 2 : 0);
    }

    private void assertUpdatesMapSharedShreddingHistory(FileFormat format, String extension)
            throws Exception {
        FileIO fileIO = LocalFileIO.create();
        ShreddingWritePlanHistory history = ShreddingWritePlanHistory.empty();
        FormatWriterFactory writerFactory =
                ShreddingWritePlanWriterFactories.createWriterFactory(
                        format, logicalRowType(), () -> history);

        Path firstFile = new Path(tempDir.toString(), UUID.randomUUID() + "." + extension);
        write(fileIO, firstFile, writerFactory, GenericRow.of(1, stringKeyMap("a", 10L)));
        assertThat(history.files()).hasSize(1);

        Path secondFile = new Path(tempDir.toString(), UUID.randomUUID() + "." + extension);
        write(
                fileIO,
                secondFile,
                writerFactory,
                GenericRow.of(2, stringKeyMap("b", 20L, "c", 30L)));
        assertThat(history.files()).hasSize(2);

        Map<String, Map<String, String>> fieldMetadata =
                ((SupportsFieldMetadata) format)
                        .readFieldMetadata(
                                new FormatReaderContext(
                                        fileIO, secondFile, fileIO.getFileSize(secondFile)));
        MapSharedShreddingFieldMeta fieldMeta =
                MapSharedShreddingUtils.deserializeMetadata(fieldMetadata.get("tags"));
        assertThat(fieldMeta.numColumns()).isEqualTo(1);
        assertThat(fieldMeta.maxRowWidth()).isEqualTo(2);
    }

    private Map<String, Map<String, String>> writeAndReadFieldMetadata(
            FileFormat format, String extension, String compression) throws IOException {
        FileIO fileIO = LocalFileIO.create();
        Path file = new Path(tempDir.toString(), UUID.randomUUID() + "." + extension);
        RowType rowType = logicalRowType();

        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = writerFactory.create(out, compression);
        writer.addElement(GenericRow.of(1, stringKeyMap("a", 10L, "b", 20L, "c", 30L)));
        writer.close();
        out.close();

        FormatReaderContext readerContext =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        return ((SupportsFieldMetadata) format).readFieldMetadata(readerContext);
    }

    private static void write(
            FileIO fileIO, Path file, FormatWriterFactory writerFactory, GenericRow row)
            throws IOException {
        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = writerFactory.create(out, "none");
        writer.addElement(row);
        writer.close();
        out.close();
    }

    private static RowType logicalRowType() {
        return DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
    }

    private static Options mapSharedShreddingOptions() {
        Options options = new Options();
        options.setString("fields.tags.map.storage-layout", "shared-shredding");
        options.setString("fields.tags.map.shared-shredding.max-columns", "2");
        return options;
    }

    private static void assertMapSharedShreddingMetadata(
            Map<String, Map<String, String>> fieldMetadata, String compression) {
        assertThat(fieldMetadata).containsKey("tags");
        assertThat(fieldMetadata.get("tags"))
                .containsEntry(
                        MapShreddingDefine.STORAGE_LAYOUT,
                        MapShreddingDefine.STORAGE_LAYOUT_SHARED_SHREDDING);

        MapSharedShreddingFieldMeta fieldMeta =
                MapSharedShreddingUtils.deserializeMetadata(fieldMetadata.get("tags"), compression);
        assertThat(fieldMeta.nameToId()).containsOnlyKeys("a", "b", "c");
        assertThat(fieldMeta.fieldToColumns()).hasSize(2);
        assertThat(fieldMeta.fieldToColumns().values())
                .containsExactlyInAnyOrder(
                        Collections.singletonList(0), Collections.singletonList(1));
        assertThat(fieldMeta.overflowFieldSet()).hasSize(1);
        assertThat(fieldMeta.numColumns()).isEqualTo(2);
        assertThat(fieldMeta.maxRowWidth()).isEqualTo(3);
    }

    private static GenericMap stringKeyMap(Object... keyValues) {
        Map<Object, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(BinaryString.fromString((String) keyValues[i]), keyValues[i + 1]);
        }
        return new GenericMap(values);
    }
}
