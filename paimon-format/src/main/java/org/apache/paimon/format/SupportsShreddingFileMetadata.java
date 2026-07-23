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

package org.apache.paimon.format;

import org.apache.paimon.format.shredding.ShreddingFileMetadata;

import java.io.IOException;
import java.util.Map;

/** Format capability for recovering shredding schema and field metadata with one file open. */
public interface SupportsShreddingFileMetadata extends SupportsFieldMetadata {

    ShreddingFileMetadata readShreddingFileMetadata(FormatReaderFactory.Context context)
            throws IOException;

    @Override
    default Map<String, Map<String, String>> readFieldMetadata(FormatReaderFactory.Context context)
            throws IOException {
        return readShreddingFileMetadata(context).fieldMetadata();
    }
}
