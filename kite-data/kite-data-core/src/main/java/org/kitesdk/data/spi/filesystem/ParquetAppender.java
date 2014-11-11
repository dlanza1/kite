/**
 * Copyright 2013 Cloudera Inc.
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
package org.kitesdk.data.spi.filesystem;

import com.google.common.base.Objects;
import com.google.common.io.Closeables;

import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.Formats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parquet.avro.AvroParquetWriter;
import parquet.hadoop.BlockSizeReachedException;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

class ParquetAppender<E extends IndexedRecord> implements FileSystemWriter.FileAppender<E> {

  private static final Logger LOG = LoggerFactory
    .getLogger(ParquetAppender.class);
//  private static final int DEFAULT_BLOCK_SIZE = 50 * 1024 * 1024;
  private static final int DEFAULT_BLOCK_SIZE = 256 * 1024 * 1024;

  private final Path path;
  private final Schema schema;
  private final FileSystem fileSystem;
  private final Configuration conf;
  private final boolean enableCompression;
  private final CompressionType compressionType;
  private final boolean file_per_block;

  private AvroParquetWriter<E> avroParquetWriter = null;
  
  public ParquetAppender(FileSystem fileSystem, Path path, Schema schema,
                         Configuration conf, CompressionType compressionType) {
	  this(fileSystem, path, schema, conf, compressionType, false);
  }
  
  public ParquetAppender(FileSystem fileSystem, Path path, Schema schema,
          Configuration conf, CompressionType compressionType, boolean file_per_block) {
	  this.fileSystem = fileSystem;
	    this.path = path;
	    this.schema = schema;
	    this.conf = conf;
	    this.enableCompression = compressionType != CompressionType.Uncompressed;
	    this.compressionType = compressionType;
	    this.file_per_block = file_per_block;
  }

  @Override
  public void open() throws IOException {
    CompressionCodecName codecName = CompressionCodecName.UNCOMPRESSED;
    if (enableCompression) {
      codecName = getCompressionCodecName();
    }

    avroParquetWriter = new AvroParquetWriter<E>(fileSystem.makeQualified(path),
        schema, codecName, DEFAULT_BLOCK_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE,
        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED, conf, file_per_block);
  }

  @Override
  public void append(E entity) throws IOException, BlockSizeReachedException {
    avroParquetWriter.write(entity);
  }

  @Override
  public void flush() {
    // Parquet doesn't (currently) expose a flush operation
  }

  @Override
  public void sync() {
    // Parquet doesn't (currently) expose a sync operation
  }

  @Override
  public void close() throws IOException {
    Closeables.close(avroParquetWriter, false);
  }

  @Override
  public void cleanup() throws IOException {
    // No cleanup tasks needed
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("path", path)
      .add("schema", schema)
      .add("fileSystem", fileSystem)
      .add("avroParquetWriter", avroParquetWriter)
      .toString();
  }

  private CompressionCodecName getCompressionCodecName() {
    switch (compressionType) {
      case Snappy:
        return CompressionCodecName.SNAPPY;

      case Lzo:
        return CompressionCodecName.LZO;

      case Deflate:
        return CompressionCodecName.GZIP;

      default:
        throw new IllegalArgumentException(String.format(
            "Unsupported compression format %s. Supported formats: %s",
            compressionType.getName(), Arrays.toString(
                Formats.PARQUET.getSupportedCompressionTypes().toArray())));
    }
  }

}
