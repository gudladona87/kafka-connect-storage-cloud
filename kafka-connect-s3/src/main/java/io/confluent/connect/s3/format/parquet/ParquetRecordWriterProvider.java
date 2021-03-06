/**
 * Copyright 2015 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.connect.s3.format.parquet;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class ParquetRecordWriterProvider
    implements io.confluent.connect.storage.format.RecordWriterProvider<S3SinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";
  private final AvroData avroData;
  private final S3Storage storage;

  ParquetRecordWriterProvider(S3Storage storage, AvroData avroData) {
    this.avroData = avroData;
    this.storage = storage;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public io.confluent.connect.storage.format.RecordWriter getRecordWriter(
      final S3SinkConnectorConfig conf,
      final String filename
  ) {
    return new io.confluent.connect.storage.format.RecordWriter() {
      final CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
      final int blockSize = 256 * 1024 * 1024;
      final int pageSize = 64 * 1024;
      final Path path = new Path(filename);
      Schema schema = null;
      ParquetWriter<GenericRecord> writer = null;
      S3OutputStream s3out;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          s3out = storage.create(filename, true);
          try {
            log.info("Opening record writer for: {}", filename);
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
            AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.builder(path);
            writer = builder.withSchema(avroSchema)
                .withCompressionCodec(compressionCodecName)
                .withPageSize(pageSize)
                .withRowGroupSize(blockSize)
                .enableDictionaryEncoding().build();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        log.trace("Sink record: {}", record.toString());
        Object value = avroData.fromConnectData(record.valueSchema(), record.value());
        try {
          writer.write((GenericRecord) value);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {
        if (writer != null) {
          try {
            writer.close();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
      }

      @Override
      public void commit() {
        try {
          //close and copy the bytes to output stream.
          writer.close();
          File f = new File(s3out.getKey());
          long n = Files.copy(f.toPath(), s3out);
          log.info("Copied {} bytes to S3", n);

          // commit to S3 and delete the file.
          s3out.commit();
          boolean isDeleted = f.delete();
          if (!isDeleted) {
            log.warn("File delete failed: {}", f.getAbsolutePath());
          }
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    };
  }
}
