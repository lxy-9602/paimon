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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.SupportsShreddingFileMetadata;
import org.apache.paimon.format.shredding.ShreddingFileMetadata;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Table-level tests for Variant shredding. */
public class VariantShreddingTableTest extends TableTestBase {

    @Test
    public void testAppendOnlyTableInfersAndRestoresShreddingSchema() throws Exception {
        Table table =
                createTable(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("payload", DataTypes.VARIANT()));

        write(table, GenericRow.of(1, GenericVariant.fromJson("{\"age\":30}")));
        write(table, GenericRow.of(2, GenericVariant.fromJson("{\"name\":\"Alice\"}")));

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileMeta> files = currentDataFiles(fileStoreTable);
        files.sort(Comparator.comparingLong(DataFileMeta::minSequenceNumber));
        assertThat(files).hasSize(2);

        ShreddingFileMetadata firstMetadata =
                readShreddingFileMetadata(fileStoreTable, files.get(0));
        ShreddingFileMetadata secondMetadata =
                readShreddingFileMetadata(fileStoreTable, files.get(1));
        assertThat(typedValueFieldNames(firstMetadata, 1)).containsExactly("age");
        assertThat(typedValueFieldNames(secondMetadata, 1)).containsExactly("age");

        Map<Integer, String> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.getVariant(1).toJson());
        }
        assertThat(actual)
                .containsEntry(1, "{\"age\":30}")
                .containsEntry(2, "{\"name\":\"Alice\"}");
    }

    @Test
    public void testAppendOnlyTableRestoresNestedVariantSchema() throws Exception {
        RowType detailsType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "payload", DataTypes.VARIANT()),
                        DataTypes.FIELD(1, "score", DataTypes.INT()));
        RowType containerType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "details", detailsType),
                        DataTypes.FIELD(1, "version", DataTypes.BIGINT()));
        Table table =
                createTable(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("container", containerType));

        write(
                table,
                GenericRow.of(
                        1,
                        GenericRow.of(
                                GenericRow.of(GenericVariant.fromJson("{\"age\":30}"), 10), 100L)));
        write(
                table,
                GenericRow.of(
                        2,
                        GenericRow.of(
                                GenericRow.of(GenericVariant.fromJson("{\"name\":\"Alice\"}"), 20),
                                200L)));

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileMeta> files = currentDataFiles(fileStoreTable);
        files.sort(Comparator.comparingLong(DataFileMeta::minSequenceNumber));
        assertThat(files).hasSize(2);
        assertThat(
                        typedValueFieldNames(
                                readShreddingFileMetadata(fileStoreTable, files.get(0)), 1, 0, 0))
                .containsExactly("age");
        assertThat(
                        typedValueFieldNames(
                                readShreddingFileMetadata(fileStoreTable, files.get(1)), 1, 0, 0))
                .containsExactly("age");

        Map<Integer, List<Object>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            InternalRow container = row.getRow(1, 2);
            InternalRow details = container.getRow(0, 2);
            actual.put(
                    row.getInt(0),
                    Arrays.asList(
                            details.getVariant(0).toJson(),
                            details.getInt(1),
                            container.getLong(1)));
        }
        assertThat(actual)
                .containsEntry(1, Arrays.asList("{\"age\":30}", 10, 100L))
                .containsEntry(2, Arrays.asList("{\"name\":\"Alice\"}", 20, 200L));
    }

    @Test
    public void testAppendOnlyTableRestoresExistingAndInfersNewVariant() throws Exception {
        Table table =
                createTable(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("payload", DataTypes.VARIANT()));

        write(table, GenericRow.of(1, GenericVariant.fromJson("{\"age\":30}")));

        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn("new_payload", DataTypes.VARIANT())),
                false);
        table = catalog.getTable(identifier());

        write(
                table,
                GenericRow.of(
                        2,
                        GenericVariant.fromJson("{\"other\":1}"),
                        GenericVariant.fromJson("{\"name\":\"Alice\"}")));

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileMeta> files = currentDataFiles(fileStoreTable);
        files.sort(Comparator.comparingLong(DataFileMeta::minSequenceNumber));
        assertThat(files).hasSize(2);
        ShreddingFileMetadata secondMetadata =
                readShreddingFileMetadata(fileStoreTable, files.get(1));
        assertThat(typedValueFieldNames(secondMetadata, 1)).containsExactly("age");
        assertThat(typedValueFieldNames(secondMetadata, 2)).containsExactly("name");

        Map<Integer, List<String>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(
                    row.getInt(0),
                    Arrays.asList(
                            row.getVariant(1).toJson(),
                            row.isNullAt(2) ? null : row.getVariant(2).toJson()));
        }
        assertThat(actual)
                .containsEntry(1, Arrays.asList("{\"age\":30}", null))
                .containsEntry(2, Arrays.asList("{\"other\":1}", "{\"name\":\"Alice\"}"));
    }

    private Table createTable(Schema.Builder builder) throws Exception {
        catalog.createTable(
                identifier(),
                builder.option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_PARQUET)
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA.key(), "true")
                        .option(
                                CoreOptions.VARIANT_RESTORE_SHREDDING_SCHEMA_FROM_HISTORY.key(),
                                "true")
                        .option(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW.key(), "1")
                        .build(),
                false);
        return catalog.getTable(identifier());
    }

    private List<DataFileMeta> currentDataFiles(FileStoreTable table) throws Exception {
        List<DataFileMeta> files = new ArrayList<>();
        for (DataSplit split : table.newSnapshotReader().read().dataSplits()) {
            files.addAll(split.dataFiles());
        }
        return files;
    }

    private ShreddingFileMetadata readShreddingFileMetadata(FileStoreTable table, DataFileMeta file)
            throws Exception {
        DataFilePathFactory pathFactory =
                table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
        FileFormat fileFormat =
                FileFormatDiscover.of(new CoreOptions(table.options())).discover(file.fileFormat());
        return ((SupportsShreddingFileMetadata) fileFormat)
                .readShreddingFileMetadata(
                        new FormatReaderContext(
                                table.fileIO(), pathFactory.toPath(file), file.fileSize()));
    }

    private List<String> typedValueFieldNames(ShreddingFileMetadata metadata, int... fieldPath) {
        RowType physicalRowType = metadata.physicalRowType();
        assertThat(physicalRowType).isNotNull();
        RowType currentType = physicalRowType;
        for (int i = 0; i < fieldPath.length - 1; i++) {
            currentType = (RowType) currentType.getTypeAt(fieldPath[i]);
        }
        RowType variantType = (RowType) currentType.getTypeAt(fieldPath[fieldPath.length - 1]);
        RowType typedValueType =
                (RowType) variantType.getField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME).type();
        return typedValueType.getFieldNames();
    }
}
