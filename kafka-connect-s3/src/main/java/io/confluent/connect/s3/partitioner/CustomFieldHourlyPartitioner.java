/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3.partitioner;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class CustomFieldHourlyPartitioner<T> extends DefaultPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(io.confluent.connect.s3.partitioner
      .CustomFieldHourlyPartitioner.class);
  private List<String> fieldNames;


  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, Object> config) {
    fieldNames = (List<String>) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    delim = StorageCommonConfig.DIRECTORY_DELIM_DEFAULT;
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    Object value = sinkRecord.value();
    if (value instanceof Struct) {
      final Schema valueSchema = sinkRecord.valueSchema();
      final Struct struct = (Struct) value;

      StringBuilder builder = new StringBuilder();
      for (String fieldName : fieldNames) {
        Object partitionKey = struct.get(fieldName);
        Type type = valueSchema.field(fieldName).schema().type();
        switch (type) {
          case INT8:
          case INT16:
          case INT32:
          case INT64:
            Number record = (Number) partitionKey;
            builder.append(fieldName + "=" + record.toString());
            break;
          case STRING:
            builder.append(fieldName + "=" + (String) partitionKey);
            break;
          case BOOLEAN:
            boolean booleanRecord = (boolean) partitionKey;
            builder.append(fieldName + "=" + Boolean.toString(booleanRecord));
            break;
          default:
            log.error("Type {} is not supported as a partition key.", type.getName());
            throw new PartitionException("Error encoding partition.");
        }
        builder.append(this.delim);
      }

      Calendar cal = Calendar.getInstance();

      builder.append("year=" + cal.get(Calendar.YEAR));
      builder.append(this.delim);

      builder.append("month=" + cal.get(Calendar.MONTH));
      builder.append(this.delim);

      builder.append("day=" + cal.get(Calendar.DAY_OF_MONTH));
      builder.append(this.delim);

      builder.append("hour=" + cal.get(Calendar.HOUR_OF_DAY));

      return builder.toString();
    } else {
      log.error("Value is not Struct type.");
      throw new PartitionException("Error encoding partition.");
    }
  }

  @Override
  public List<T> partitionFields() {
    if (partitionFields == null) {
      partitionFields = newSchemaGenerator(config).newPartitionFields(
          Utils.join(fieldNames, ",")
      );
    }
    return partitionFields;
  }
}
