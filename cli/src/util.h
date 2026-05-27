/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <iostream>
#include <sys/resource.h>
#include <unistd.h>
#if defined(__linux__)
#include <fstream>
 #include <unistd.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#endif

#ifdef ARROW_ORC
#include "arrow/adapters/orc/adapter.h"
#endif
#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/csv/api.h"
#include "arrow/io/api.h"
#include "arrow/json/api.h"
#include "graphar/api/arrow_writer.h"
#include "graphar/api/high_level_writer.h"
#include "graphar/graph_info.h"
#include "parquet/arrow/reader.h"

#include <vector>
#include <string>
#include <cstdint>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdexcept>
#include <cstring>
#include <filesystem>
#include <sys/file.h>

namespace fs = std::filesystem;


std::string ConcatEdgeTriple(const std::string& src_type,
                             const std::string& edge_type,
                             const std::string& dst_type) {
  return src_type + REGULAR_SEPARATOR + edge_type + REGULAR_SEPARATOR +
         dst_type;
}

graphar::ValidateLevel StringToValidateLevel(const std::string& level) {
  if (level == "no") {
    return graphar::ValidateLevel::no_validate;
  } else if (level == "weak") {
    return graphar::ValidateLevel::weak_validate;
  } else if (level == "strong") {
    return graphar::ValidateLevel::strong_validate;
  } else {
    throw std::runtime_error("Invalid validate level: " + level);
  }
}

// Utility function to filter the columns from a table
std::shared_ptr<arrow::Table> SelectColumns(
    const std::shared_ptr<arrow::Table>& table,
    const std::vector<std::string>& column_names) {
  if (column_names.empty()) {
    throw std::runtime_error("No column names provided.");
  }
  std::vector<int> indices;
  indices.reserve(column_names.size());
  for (const auto& name : column_names) {
    auto column_index = table->schema()->GetFieldIndex(name);
    if (column_index != -1) {
      indices.push_back(column_index);
    }
  }

  if (indices.empty()) {
    throw std::runtime_error("None of the column names matched the schema.");
  }

  return table->SelectColumns(indices).ValueOrDie();
}

std::shared_ptr<arrow::Table> GetDataFromParquetFile(
    const std::string& path, const std::vector<std::string>& column_names) {
  // Open the Parquet file
  // Create a Parquet FileReader
  std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
  auto status = graphar::util::OpenParquetArrowReader(
      path, arrow::default_memory_pool(), &parquet_reader);
  if (!status.ok()) {
    throw std::runtime_error("Failed to create Parquet FileReader: " +
                             status.ToString());
  }

  // Retrieve the Arrow schema from the Parquet file
  std::shared_ptr<arrow::Schema> schema;
  status = parquet_reader->GetSchema(&schema);
  if (!status.ok()) {
    throw std::runtime_error("Failed to retrieve schema from Parquet file: " +
                             status.ToString());
  }

  // Map column names to their indices in the schema
  std::vector<int> column_indices;
  column_indices.reserve(column_names.size());
  for (const auto& col_name : column_names) {
    int64_t index = schema->GetFieldIndex(col_name);
    if (index == -1) {
      throw std::runtime_error("Column not found in schema: " + col_name);
    }
    column_indices.push_back(index);
  }

  // Read the table with the selected columns
  std::shared_ptr<arrow::Table> table;
  status = parquet_reader->ReadTable(column_indices, &table);
  if (!status.ok()) {
    throw std::runtime_error("Failed to read table from Parquet file: " +
                             status.ToString());
  }

  return table;
}

std::shared_ptr<arrow::Table> GetDataFromCsvFile(
    const std::string& path, const std::vector<std::string>& column_names,
    const char delimiter) {
  // Open the CSV file
  auto input_result =
      arrow::io::ReadableFile::Open(path, arrow::default_memory_pool());
  if (!input_result.ok()) {
    throw std::runtime_error("Failed to open CSV file: " +
                             input_result.status().ToString());
  }
  std::shared_ptr<arrow::io::ReadableFile> input = input_result.ValueOrDie();

  // Define CSV parse options with the specified delimiter
  arrow::csv::ParseOptions parse_options = arrow::csv::ParseOptions::Defaults();
  parse_options.delimiter = delimiter;

  // Define CSV convert options to include only the specified columns
  arrow::csv::ConvertOptions convert_options =
      arrow::csv::ConvertOptions::Defaults();
  convert_options.include_columns = column_names;

  // Optional: Define CSV read options (using defaults here)
  arrow::csv::ReadOptions read_options = arrow::csv::ReadOptions::Defaults();

  // Create a CSV TableReader using  IOContext
  arrow::io::IOContext io_context(arrow::default_memory_pool());
  arrow::Result<std::shared_ptr<arrow::csv::TableReader>> reader_result =
      arrow::csv::TableReader::Make(io_context, input, read_options,
                                    parse_options, convert_options);

  if (!reader_result.ok()) {
    throw std::runtime_error("Failed to create CSV TableReader: " +
                             reader_result.status().ToString());
  }
  std::shared_ptr<arrow::csv::TableReader> reader = reader_result.ValueOrDie();

  // Read the table
  arrow::Result<std::shared_ptr<arrow::Table>> table_result = reader->Read();
  if (!table_result.ok()) {
    throw std::runtime_error("Failed to read table from CSV file: " +
                             table_result.status().ToString());
  }
  std::shared_ptr<arrow::Table> table = table_result.ValueOrDie();

  // Optional: Validate that all requested columns are present
  auto schema = table->schema();
  for (const auto& col_name : column_names) {
    if (schema->GetFieldByName(col_name) == nullptr) {
      throw std::runtime_error("Column not found in CSV file: " + col_name);
    }
  }

  return table;
}

#ifdef ARROW_ORC
std::shared_ptr<arrow::Table> GetDataFromOrcFile(
    const std::string& path, const std::vector<std::string>& column_names) {
  // Open the ORC file
  auto infile =
      arrow::io::ReadableFile::Open(path, arrow::default_memory_pool())
          .ValueOrDie();

  // Create an ORC file reader
  std::unique_ptr<arrow::adapters::orc::ORCFileReader> orc_reader =
      arrow::adapters::orc::ORCFileReader::Open(infile,
                                                arrow::default_memory_pool())
          .ValueOrDie();

  // Read the table with the selected columns
  arrow::Result<std::shared_ptr<arrow::Table>> table_result =
      orc_reader->Read(column_names);
  if (!table_result.ok()) {
    throw std::runtime_error("Failed to read table from ORC file: " +
                             table_result.status().ToString());
  }
  std::shared_ptr<arrow::Table> table = table_result.ValueOrDie();

  // Optional: Validate that all requested columns are present
  auto schema = table->schema();
  for (const auto& col_name : column_names) {
    if (schema->GetFieldByName(col_name) == nullptr) {
      throw std::runtime_error("Column not found in ORC file: " + col_name);
    }
  }

  return table;
}
#endif

std::shared_ptr<arrow::Table> GetDataFromJsonFile(
    const std::string& path, const std::vector<std::string>& column_names) {
  //  Open the JSON file
  auto infile =
      arrow::io::ReadableFile::Open(path, arrow::default_memory_pool())
          .ValueOrDie();

  // Define JSON read options (using defaults here)
  arrow::json::ReadOptions read_options = arrow::json::ReadOptions::Defaults();

  // Define JSON parse options (using defaults here)
  arrow::json::ParseOptions parse_options =
      arrow::json::ParseOptions::Defaults();

  // Create a JSON TableReader
  std::shared_ptr<arrow::json::TableReader> json_reader =
      arrow::json::TableReader::Make(arrow::default_memory_pool(), infile,
                                     arrow::json::ReadOptions::Defaults(),
                                     arrow::json::ParseOptions::Defaults())
          .ValueOrDie();

  // Read the table
  arrow::Result<std::shared_ptr<arrow::Table>> table_result =
      json_reader->Read();
  if (!table_result.ok()) {
    throw std::runtime_error("Failed to read table from ORC file: " +
                             table_result.status().ToString());
  }
  std::shared_ptr<arrow::Table> table = table_result.ValueOrDie();

  table = SelectColumns(table, column_names);

  // Optional: Validate that all requested columns are present
  // TODO: must be equal
  auto schema = table->schema();
  for (const auto& col_name : column_names) {
    if (schema->GetFieldByName(col_name) == nullptr) {
      throw std::runtime_error("Column not found in JSON file: " + col_name);
    }
  }

  return table;
}

/*arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> OpenParquetAsBatch(
    const std::string& path, const std::vector<std::string>& column_names) {

  ARROW_ASSIGN_OR_RAISE(auto input, arrow::io::ReadableFile::Open(path));
  std::unique_ptr<parquet::arrow::FileReader> parquet_reader;

  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &parquet_reader));

  // Retrieve the Arrow schema from the Parquet file
  std::shared_ptr<arrow::Schema> schema;
  ARROW_RETURN_NOT_OK(parquet_reader->GetSchema(&schema));

  // Map column names to their indices in the schema
  std::vector<int> column_indices;
  column_indices.reserve(column_names.size());
  for (const auto& col_name : column_names) {
    int64_t index = schema->GetFieldIndex(col_name);
    if (index == -1) {
      return arrow::Status::Invalid("Column not found in schema: " + col_name);
    }
    column_indices.push_back(index);
  }

  // Create batch reader
  std::vector<int> row_groups(parquet_reader->num_row_groups());
  std::iota(row_groups.begin(), row_groups.end(), 0);

  std::shared_ptr<arrow::RecordBatchReader> batch_reader;
  ARROW_RETURN_NOT_OK(
      parquet_reader->GetRecordBatchReader(
          row_groups,
          column_indices,
          &batch_reader));

  return batch_reader;
}

std::shared_ptr<arrow::RecordBatchReader> GetDataAsBatch(
        const std::string& path, const std::vector<std::string>& column_names,
        const char& delimiter=',', const std::string& file_type="parquet") {

    if (file_type == "parquet") {
      auto result = OpenParquetAsBatch(path, column_names);
      if (!result.ok()) {
        throw std::runtime_error("Colud not open file: " + path);
      }
      return result.ValueOrDie();;
    } else {
      // TODO: add csv, orc, json, any imprtant format
      throw std::runtime_error("Unsupported file type: " + file_type);
    }
}*/

std::shared_ptr<arrow::Table> GetDataFromFile(
    const std::string& path, const std::vector<std::string>& column_names,
    const char& delimiter, const std::string& file_type) {
  // TODO: use explicit schema
  // TODO: use switch case
  if (file_type == "parquet") {
    return GetDataFromParquetFile(path, column_names);
  } else if (file_type == "csv") {
    return GetDataFromCsvFile(path, column_names, delimiter);
#ifdef ARROW_ORC
  } else if (file_type == "orc") {
    return GetDataFromOrcFile(path, column_names);
#endif
  } else if (file_type == "json") {
    return GetDataFromJsonFile(path, column_names);
  } else {
    throw std::runtime_error("Unsupported file type: " + file_type);
  }
}


/*==================== Bin files in-out functions & settings ====================*/

#if __BYTE_ORDER != __LITTLE_ENDIAN
#  error "Unsupported endianness (only little endian is supported)"
#endif

struct BinHeader {
    uint32_t magic = 0x42494E31;
    uint64_t count = 0;
    uint32_t element_size = sizeof(int64_t);

    BinHeader()
        : magic(0x42494E31),
          count(0),
          element_size(sizeof(int64_t)) {}
};

constexpr uint32_t BIN_MAGIC = 0x42494E31;
constexpr size_t BIN_HEADER_SIZE = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t);


void clear_directory(const std::string& path) {
    for (const auto& entry : fs::directory_iterator(path)) {
        fs::remove_all(entry.path());
    }
}


void clear_file(const std::string& path) {
    std::error_code ec;
    if (!fs::remove(path, ec)) {
        if (ec) {
            throw std::runtime_error("remove failed: " + ec.message());
        }
    }
}

class FileDescriptor {
    int fd_;
public:
    explicit FileDescriptor(int fd) : fd_(fd) {}
    ~FileDescriptor() {
        if (fd_ != -1) close(fd_);
    }
    
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;
    
    FileDescriptor(FileDescriptor&& other) noexcept : fd_(other.fd_) {
        other.fd_ = -1;
    }
    
    int get() const { return fd_; }
    
    void lock_exclusive() {
        if (flock(fd_, LOCK_EX) != 0) {
            throw std::runtime_error("Failed to lock file");
        }
    }
    
    void unlock() {
        flock(fd_, LOCK_UN);
    }
};

FileDescriptor open_bin(const std::string& path) {
    int fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd == -1) {
        throw std::runtime_error("open failed: " + path);
    }
    return FileDescriptor(fd);
}


ssize_t write_exact(int fd, const void* buf, size_t size, size_t offset) {
    size_t written = 0;

    while (written < size) {
        ssize_t w = pwrite(
            fd,
            static_cast<const char*>(buf) + written,
            size - written,
            offset + written
        );

        if (w < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("pwrite failed");
        }

        if (w == 0) {
            throw std::runtime_error("pwrite wrote 0 bytes");
        }
        written += w;
    }
    return written;
}


void read_exact(int fd, void* buf, size_t size, off_t offset) {
    size_t read_bytes = 0;

    while (read_bytes < size) {
        ssize_t r = pread(
            fd,
            static_cast<char*>(buf) + read_bytes,
            size - read_bytes,
            offset + read_bytes
        );

        if (r < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("pread failed");
        }

        if (r == 0) {
            throw std::runtime_error("unexpected EOF");
        }

        read_bytes += r;
    }
}


void write_header_bin(int fd, const BinHeader& h) {
    size_t offset = 0;

    offset += write_exact(fd, &h.magic, sizeof(h.magic), offset);
    offset += write_exact(fd, &h.count, sizeof(h.count), offset);
    offset += write_exact(fd, &h.element_size, sizeof(h.element_size), offset);

    if (offset != BIN_HEADER_SIZE) {
        throw std::runtime_error("header write failed");
    }
}


BinHeader read_header_bin(int fd) {
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < BIN_HEADER_SIZE) {
        return BinHeader();
    }

    BinHeader h;
    size_t offset = 0;

    read_exact(fd, &h.magic, sizeof(h.magic), offset);
    offset += sizeof(h.magic);

    read_exact(fd, &h.count, sizeof(h.count), offset);
    offset += sizeof(h.count);

    read_exact(fd, &h.element_size, sizeof(h.element_size), offset);

    if (h.magic != BIN_MAGIC) {
        throw std::runtime_error("Invalid file format");
    }

    if (h.element_size != sizeof(int64_t)) {
        throw std::runtime_error("Unexpected element size");
    }

    return h;
}

void append_to_bin(const std::vector<int64_t>& data, const std::string& path) {
    FileDescriptor fd = open_bin(path);
    fd.lock_exclusive();

    BinHeader header = read_header_bin(fd.get());

    uint64_t old_count = header.count;
    uint64_t new_count = old_count + data.size();

    off_t offset = BIN_HEADER_SIZE + old_count * sizeof(int64_t);
    size_t bytes = data.size() * sizeof(int64_t);
    write_exact(fd.get(), data.data(), bytes, offset);

    header.count = new_count;
    write_header_bin(fd.get(), header);

    fsync(fd.get());
}


class FileStream {
public:
    explicit FileStream(const std::string& path)
        : fd_(open_bin(path)), path_(path)
    {
        header_ = read_header_bin(fd_.get());
        offset_ = BIN_HEADER_SIZE;
    }

    bool next(int64_t& value) {
        if (read_elements_ >= header_.count) {
            return false;
        }

        if (pos_ == size_) {
            refill();
            if (size_ == 0) {
                return false;
            }
        }

        value = buffer_[pos_++];
        ++read_elements_;
        return true;
    }

    uint64_t count() const {
        return header_.count;
    }

    const std::string& get_path() {return path_;}

private:
    void refill() {
        uint64_t remaining = header_.count - read_elements_;
        if (remaining == 0) {
            size_ = 0;
            return;
        }

        size_t elements_to_read = std::min<uint64_t>(
            BUF_SIZE,
            remaining
        );

        size_t bytes_to_read = elements_to_read * sizeof(int64_t);

        ssize_t r = pread(fd_.get(), buffer_, bytes_to_read, offset_);

        if (r < 0) {
            if (errno == EINTR) return refill();
            throw std::runtime_error("pread failed");
        }

        if (r == 0) {
            throw std::runtime_error("unexpected EOF");
        }

        size_ = r / sizeof(int64_t);
        pos_ = 0;
        offset_ += r;
    }

    FileDescriptor fd_;
    BinHeader header_;
    std::string path_;

    static constexpr size_t BUF_SIZE = 4096;
    int64_t buffer_[BUF_SIZE];

    size_t pos_ = 0;
    size_t size_ = 0;
    uint64_t read_elements_ = 0;
    off_t offset_ = 0;
};


int64_t get_count_bin(const std::string& path) {
    FileDescriptor fd_guard = open_bin(path);
    fd_guard.lock_exclusive();
    
    try {
        BinHeader header = read_header_bin(fd_guard.get());
        return static_cast<int64_t>(header.count);
    } catch (...) {
        fd_guard.unlock();
        throw;
    }
}


/*==================== Table editing  ====================*/
std::shared_ptr<arrow::Table> ChangeNameAndDataType(
    const std::shared_ptr<arrow::Table>& table,
    const std::unordered_map<
        std::string, std::pair<std::string, std::shared_ptr<arrow::DataType>>>&
        columns_to_change) {
  // Retrieve original schema and number of columns
  auto original_schema = table->schema();
  int64_t num_columns = table->num_columns();

  // Prepare vectors for new schema fields and new column data
  std::vector<std::shared_ptr<arrow::Field>> new_fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;
  new_fields.reserve(num_columns);
  new_columns.reserve(num_columns);

  for (int64_t i = 0; i < num_columns; ++i) {
    auto original_field = original_schema->field(i);
    auto original_column = table->column(i);  // This is a ChunkedArray

    std::string original_name = original_field->name();
    std::shared_ptr<arrow::DataType> original_type = original_field->type();

    // Check if this column needs to be changed
    auto it = columns_to_change.find(original_name);
    if (it != columns_to_change.end()) {
      std::string new_name = it->second.first;
      std::shared_ptr<arrow::DataType> new_type = it->second.second;

      bool name_changed = (new_name != original_name);
      bool type_changed = !original_type->Equals(*new_type);

      std::shared_ptr<arrow::ChunkedArray> new_chunked_array;

      // If data type needs to be changed, cast each chunk
      if (type_changed) {
        std::vector<std::shared_ptr<arrow::Array>> casted_chunks;
        casted_chunks.reserve(original_column->num_chunks());
        for (const auto& chunk : original_column->chunks()) {
          // Perform type casting using Compute API
          arrow::compute::CastOptions cast_options;
          cast_options.allow_int_overflow = false;  // Set as needed
          cast_options.allow_time_truncate = true;

          auto cast_result =
              arrow::compute::Cast(*chunk, new_type, cast_options);
          if (!cast_result.ok()) {
            std::cerr << cast_result.status().ToString() << std::endl;
            throw std::runtime_error("Failed to cast column data.");
          }
          casted_chunks.push_back(cast_result.ValueOrDie());
        }
        // Create a new ChunkedArray with casted chunks
        new_chunked_array =
            std::make_shared<arrow::ChunkedArray>(casted_chunks, new_type);
      } else {
        // If type is not changed, keep the original column
        new_chunked_array = original_column;
      }

      // Create a new Field with the updated name and type
      auto new_field =
          arrow::field(new_name, type_changed ? new_type : original_type,
                       original_field->nullable());
      new_fields.push_back(new_field);
      new_columns.push_back(new_chunked_array);
    } else {
      // Columns not in the change map remain unchanged
      new_fields.push_back(original_field);
      new_columns.push_back(original_column);
    }
  }

  // Create the new schema
  auto new_schema = arrow::schema(new_fields);

  // Construct the new table with updated schema and columns
  auto new_table = arrow::Table::Make(new_schema, new_columns);

  return new_table;
}

std::shared_ptr<arrow::Table> MergeTables(
    const std::vector<std::shared_ptr<arrow::Table>>& tables) {
  // Check if tables vector is not empty
  if (tables.empty()) {
    throw std::runtime_error("No tables to merge.");
  }

  // Check if all tables have the same number of rows
  int64_t num_rows = tables[0]->num_rows();
  for (const auto& table : tables) {
    if (table->num_rows() != num_rows) {
      throw std::runtime_error("All tables must have the same number of rows.");
    }
  }

  // Prepare a vector to hold all the columns from the input tables
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
  int64_t num_columns = 0;
  for (const auto& table : tables) {
    num_columns += table->num_columns();
  }
  fields.reserve(num_columns);
  columns.reserve(num_columns);

  for (const auto& table : tables) {
    for (int64_t i = 0; i < table->num_columns(); ++i) {
      fields.push_back(table->schema()->field(i));
      columns.push_back(table->column(i));
    }
  }

  // Create a new schema and table with merged columns
  auto merged_schema = std::make_shared<arrow::Schema>(fields);
  auto merged_table = arrow::Table::Make(merged_schema, columns, num_rows);

  return merged_table;
}


template <typename KeyArrayType>
void FillMap(const std::shared_ptr<KeyArrayType>& keys,
             const std::shared_ptr<arrow::Int64Array>& values,
             std::unordered_map<int64_t, graphar::IdType>& result) {
    for (int64_t i = 0; i < keys->length(); ++i) {
        if (keys->IsNull(i) || values->IsNull(i))
            throw std::runtime_error("Null key or value in vertex PK or index columns.");
        result[static_cast<int64_t>(keys->Value(i))] = values->Value(i);
    }
}

std::unordered_map<int64_t, graphar::IdType>
TableToUnorderedMapInt64(const std::shared_ptr<arrow::Table>& table,
                    const std::string& key_column_name,
                    const std::string& value_column_name) {
  auto combined_table = table->CombineChunks().ValueOrDie();  // terrible decision
  // Get the column indices
  auto key_column_idx =
      combined_table->schema()->GetFieldIndex(key_column_name);
  auto value_column_idx =
      combined_table->schema()->GetFieldIndex(value_column_name);
  if (key_column_idx == -1) {
    throw std::runtime_error("Key column '" + key_column_name +
                             "' not found in the table.");
  }
  if (value_column_idx == -1) {
    throw std::runtime_error("Value column '" + value_column_name +
                             "' not found in the table.");
  }

  // Extract the columns
  auto key_column = combined_table->column(key_column_idx);
  auto value_column = combined_table->column(value_column_idx);

  std::unordered_map<int64_t, graphar::IdType> result;
  result.reserve(key_column->length());

  // Ensure both columns have the same length
  if (key_column->length() != value_column->length()) {
    throw std::runtime_error("Key and value columns have different lengths.");
  }

  auto key_chunk = key_column->chunk(0);
  auto value_chunk = value_column->chunk(0);

  switch (key_chunk->type_id()) {
    case arrow::Type::INT64:
        FillMap(std::static_pointer_cast<arrow::Int64Array>(key_chunk),
                std::static_pointer_cast<arrow::Int64Array>(value_chunk),
                result);
        break;

    case arrow::Type::INT32:
        FillMap(std::static_pointer_cast<arrow::Int32Array>(key_chunk),
                std::static_pointer_cast<arrow::Int64Array>(value_chunk),
                result);
        break;

    default:
        throw std::runtime_error("Unsupported key type");
  }
  return result;
}

std::unordered_map<std::shared_ptr<arrow::Scalar>, graphar::IdType,
                   arrow::Scalar::Hash, arrow::Scalar::PtrsEqual>
TableToUnorderedMap(const std::shared_ptr<arrow::Table>& table,
                    const std::string& key_column_name,
                    const std::string& value_column_name) {
  auto combined_table = table->CombineChunks().ValueOrDie();
  // Get the column indices
  auto key_column_idx =
      combined_table->schema()->GetFieldIndex(key_column_name);
  auto value_column_idx =
      combined_table->schema()->GetFieldIndex(value_column_name);
  if (key_column_idx == -1) {
    throw std::runtime_error("Key column '" + key_column_name +
                             "' not found in the table.");
  }
  if (value_column_idx == -1) {
    throw std::runtime_error("Value column '" + value_column_name +
                             "' not found in the table.");
  }

  // Extract the columns
  auto key_column = combined_table->column(key_column_idx);
  auto value_column = combined_table->column(value_column_idx);

  std::unordered_map<std::shared_ptr<arrow::Scalar>, graphar::IdType,
                     arrow::Scalar::Hash, arrow::Scalar::PtrsEqual>
      result;

  // Ensure both columns have the same length
  if (key_column->length() != value_column->length()) {
    throw std::runtime_error("Key and value columns have different lengths.");
  }

  // Iterate over each row and populate the map
  for (int64_t i = 0; i < key_column->length(); ++i) {
    auto key_column_chunk = key_column->chunk(0);
    auto value_column_chunk = value_column->chunk(0);
    // Check for nulls
    if (key_column_chunk->IsNull(i)) {
      throw std::runtime_error("Null key value at index " + std::to_string(i) +
                               " in " + key_column_name);
    }
    if (value_column_chunk->IsNull(i)) {
      throw std::runtime_error("Null value at index " + std::to_string(i) +
                               " in " + value_column_name);
    }

    // Extract key and value using the helper function
    auto key = key_column_chunk->GetScalar(i).ValueOrDie();
    auto value = std::static_pointer_cast<arrow::Int64Scalar>(
                     value_column_chunk->GetScalar(i).ValueOrDie())
                     ->value;
    result.emplace(key, value);
  }

  return result;
}

template <graphar::Type type>
graphar::Status CastToAny(std::shared_ptr<arrow::Array> array, std::any& any,
                          int64_t index) {  // NOLINT
  if (array->IsNull(index)) {
    any = std::any();
    return graphar::Status::OK();
  }
  using ArrayType = typename graphar::TypeToArrowType<type>::ArrayType;
  auto column = std::dynamic_pointer_cast<ArrayType>(array);
  any = column->GetView(index);
  return graphar::Status::OK();
}

template <>
graphar::Status CastToAny<graphar::Type::STRING>(
    std::shared_ptr<arrow::Array> array, std::any& any,
    int64_t index) {  // NOLINT
  auto column = std::dynamic_pointer_cast<arrow::LargeStringArray>(array);
  any = column->GetString(index);
  return graphar::Status::OK();
}

graphar::Status TryToCastToAny(const std::shared_ptr<graphar::DataType>& type,
                               std::shared_ptr<arrow::Array> array,
                               std::any& any, int64_t index = 0) {  // NOLINT
  switch (type->id()) {
  case graphar::Type::BOOL:
    return CastToAny<graphar::Type::BOOL>(array, any, index);
  case graphar::Type::INT32:
    return CastToAny<graphar::Type::INT32>(array, any, index);
  case graphar::Type::INT64:
    return CastToAny<graphar::Type::INT64>(array, any, index);
  case graphar::Type::FLOAT:
    return CastToAny<graphar::Type::FLOAT>(array, any, index);
  case graphar::Type::DOUBLE:
    return CastToAny<graphar::Type::DOUBLE>(array, any, index);
  case graphar::Type::STRING:
    return CastToAny<graphar::Type::STRING>(array, any, index);
  case graphar::Type::DATE:
    return CastToAny<graphar::Type::DATE>(array, any, index);
  case graphar::Type::TIMESTAMP:
    return CastToAny<graphar::Type::TIMESTAMP>(array, any, index);
  default:
    return graphar::Status::TypeError("Unsupported type.");
  }
  return graphar::Status::OK();
}

class MemUsage {
public:
  MemUsage();
  long GetMaxMemoryUsageInMb() const;
  long GetCurrentMemoryUsageInMb() const;
  void print(bool up = false, bool down = true) const;
private:
  static long GetMaxRssInKBytes();
  static long GetCurrentRssInKBytes();
  long max_memory_usage_, current_memory_usage_;
};

MemUsage::MemUsage() {
  max_memory_usage_ = GetMaxRssInKBytes();
  current_memory_usage_ = GetCurrentRssInKBytes();
}

long MemUsage::GetMaxMemoryUsageInMb() const {
  return (GetMaxRssInKBytes() - max_memory_usage_) / 1024L;
}

long MemUsage::GetCurrentMemoryUsageInMb() const {
  return (GetCurrentRssInKBytes() - current_memory_usage_) / 1024L;
}

long MemUsage::GetMaxRssInKBytes() {
  long rss_value = 0;
  rusage usage;
  if (getrusage(RUSAGE_SELF, &usage) == 0) {
    rss_value = usage.ru_maxrss;
#ifdef __APPLE__
    rss_value /= 1024L;
#endif
  }
  return rss_value;
}

long MemUsage::GetCurrentRssInKBytes() {
#if defined(__linux__)
  std::ifstream statm("/proc/self/statm");
  long pages_resident = 0;
  statm.ignore(1, ' ');
  statm >> pages_resident;

  long page_size_kb = sysconf(_SC_PAGESIZE) / 1024;
  return pages_resident * page_size_kb;

#elif defined(__APPLE__)
  mach_task_basic_info info;
  mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                (task_info_t)&info, &infoCount) != KERN_SUCCESS)
    return 0;

  return info.resident_size / 1024;

#else
  // Платформа не поддерживается
   return 0;
#endif
}

void MemUsage::print(bool up, bool down) const {
  auto max_m = GetMaxMemoryUsageInMb();
  auto cur_m = GetCurrentMemoryUsageInMb();
  if (!up || !down) {
    if (up) {
      std::cout << "------\n";
    }
    std::cout << "| Max: " << max_m << "Mb\t\t";
    std::cout << "| Current: " << cur_m << "Mb\n";
    if (down) {
       std::cout << "------" << std::endl;
    }
  } else {
    std::cout << "| Max: " << max_m << "Mb\t\t";
    std::cout << "| Current: " << cur_m << "Mb\n";
    std::cout << "------\n";
    std::cout << "| Max: " << max_m << "Mb\t\t";
    std::cout << "| Current: " << cur_m << "Mb\n";
  }
};
