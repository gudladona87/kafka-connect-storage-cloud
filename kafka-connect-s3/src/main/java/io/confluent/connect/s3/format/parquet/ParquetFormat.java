/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.connect.s3.format.parquet;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;
import org.apache.hadoop.fs.Path;

public class ParquetFormat
    implements io.confluent.connect.storage.format.Format<S3SinkConnectorConfig, Path> {
  private final AvroData avroData;
  private final S3Storage storage;

  // DO NOT change this signature, it is required for instantiation via reflection
  public ParquetFormat(S3Storage storage) {
    this.storage = storage;
    this.avroData = new AvroData(
        storage.conf().getInt(S3SinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG)
    );
  }

  @Override
  public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
    return new ParquetRecordWriterProvider(storage, avroData);
  }

  @Override
  public SchemaFileReader<S3SinkConnectorConfig, Path> getSchemaFileReader() {
    return new ParquetFileReader(avroData);
  }

  @Override
  public HiveFactory getHiveFactory() {
    return null;
  }
}
