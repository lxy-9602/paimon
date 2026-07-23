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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Historical and newly completed file metadata for a shredding writer. */
public class ShreddingWritePlanHistory {

    public static final int MAX_HISTORY_FILES = 20;

    private final List<ShreddingFileMetadata> files;

    public ShreddingWritePlanHistory(List<ShreddingFileMetadata> files) {
        int fromIndex = Math.max(0, files.size() - MAX_HISTORY_FILES);
        this.files = new ArrayList<>(files.subList(fromIndex, files.size()));
    }

    public static ShreddingWritePlanHistory empty() {
        return new ShreddingWritePlanHistory(Collections.emptyList());
    }

    public List<ShreddingFileMetadata> files() {
        return Collections.unmodifiableList(files);
    }

    public void add(ShreddingFileMetadata file) {
        if (files.size() == MAX_HISTORY_FILES) {
            files.remove(0);
        }
        files.add(file);
    }

    public boolean isEmpty() {
        return files.isEmpty();
    }
}
