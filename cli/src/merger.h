#pragma once

#include <iostream>
#include <filesystem>
#include <string>
#include <set>
#include <pybind11/pybind11.h>
#include "pybind11/stl.h"
#include "util.h"
#include "graphar/high-level/edges_builder.h"
#include "graphar/arrow/chunk_writer.h"
#include "graphar/graph_info.h"
#include "importer.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <optional>
#include <omp.h>
#include <endian.h>

namespace py = pybind11;

struct EdgeSmall {
    int64_t src = -1;
    int64_t dst = -1;
    int64_t row = -1;

    bool operator<(const EdgeSmall& other) const {
        return src == other.src ? dst < other.dst : src < other.src;
    }

    bool operator==(const EdgeSmall& other) const {
        return src == other.src && dst == other.dst;
    }
};

std::optional<int> extract_tailing_number(const std::filesystem::path& filename) {
    std::string name = filename.stem().string();

    int end = static_cast<int>(name.size()) - 1;
    while (end >= 0 && std::isdigit(static_cast<unsigned char>(name[end]))) {
        end--;
    }

    if (end == static_cast<int>(name.size()) - 1)
        return std::nullopt;

    std::string number = name.substr(end + 1);
    return std::stoi(number);
}

/*==================== Iterating vectors or files ====================*/
class VectorStream {
public:
    VectorStream(std::vector<std::vector<std::vector<int64_t>>>& data, int chunk_idx)
        : data_(data), idx_(chunk_idx) {}

    bool next(int64_t& value) {
        while (pos_ >= data_[th][idx_].size()) {
            // clear used data (we guarantee, that no other thread will ever need this data)
            data_[th][idx_].clear();

            // move to the next thread output
            ++th; 
            pos_ = 0;
            if (th >= data_.size()) return false;
        }
        value = data_[th][idx_][pos_++];
        return true;
    }

private:
    std::vector<std::vector<std::vector<int64_t>>>& data_;
    size_t pos_ = 0, th = 0;
    int idx_;
};


template <typename F>
void with_streamer(bool use_file,
                   std::vector<std::vector<std::vector<int64_t>>>& vec,
                   const std::string& filename, int chunk_idx,
                   F&& f) {
    if (use_file) {
        FileStream s(filename);
        f(s);
    } else {
        VectorStream s(vec, chunk_idx);
        f(s);
    }
}

template <typename ArrowArrayType>
void MapPK2row(const std::shared_ptr<arrow::ChunkedArray>& column,
               std::unordered_map<int64_t, graphar::IdType>& map) {

    int64_t row_offset = 0;
    for (int64_t chunk_idx = 0; chunk_idx < column->num_chunks(); ++chunk_idx) {
        auto chunk = column->chunk(chunk_idx);
        auto arr = std::static_pointer_cast<ArrowArrayType>(chunk); 
        const auto* data = arr->raw_values();

        for (int64_t i = 0; i < arr->length(); ++i) {
            map[static_cast<int64_t>(data[i])] = row_offset++;
        }
    }
}


template <typename KeyColumnType, typename ValueColumnType>
void MapValues(const std::string& key_column_name,
               const std::string& value_column_name,
               const std::shared_ptr<arrow::Table> input_table,
               std::unordered_map<int64_t, graphar::IdType>& map) {

    std::shared_ptr<arrow::Table> table = input_table->CombineChunks().ValueOrDie();

    auto key_col_ptr = table->GetColumnByName(key_column_name);
    auto val_col_ptr = table->GetColumnByName(value_column_name);

    if (!key_col_ptr) {
        throw std::runtime_error("MapValues(): key column '" + key_column_name + "' not found in table");
    }
    if (!val_col_ptr) {
        throw std::runtime_error("MapValues(): value column '" + value_column_name + "' not found in table");
    }

    auto key_chunk = std::static_pointer_cast<KeyColumnType>(key_col_ptr->chunk(0));
    auto val_chunk = std::static_pointer_cast<ValueColumnType>(val_col_ptr->chunk(0));

    if (key_chunk->length() != val_chunk->length()) {
        throw std::runtime_error("MapValues(): Key and value column lengths do not match (" 
                                 + std::to_string(key_chunk->length()) + "!=" 
                                 + std::to_string(val_chunk->length()) + ")");
    }

    for (int64_t i = 0; i < key_chunk->length(); ++i) {
        if (key_chunk->IsNull(i) || val_chunk->IsNull(i)) {
            continue;
        }

        int64_t key = static_cast<int64_t>(key_chunk->Value(i));
        int64_t value = static_cast<int64_t>(val_chunk->Value(i));

        map[key] = value;
    }
}

/**
 * Having src&dst property of edge, finds it src&dst GraphAr index using
 * prop_index_map and saves in edge_translation[row_number_in_input_table].
 * The function suggests that CombineChunks() was already applied to the 
 * table.
 */
template <typename SrcColumnType, typename DstColumnType>
void MakeEdgeData(const std::shared_ptr<arrow::ChunkedArray> src_column,
                  const std::shared_ptr<arrow::ChunkedArray> dst_column,
                  std::vector<EdgeSmall>& edge_translation,
                  const std::unordered_map<int64_t, graphar::IdType>& src_prop_index_map,
                  const std::unordered_map<int64_t, graphar::IdType>& dst_prop_index_map,
                  const int num_threads = 1) {

    auto src_chunk = std::static_pointer_cast<SrcColumnType>(src_column->chunk(0));
    auto dst_chunk = std::static_pointer_cast<DstColumnType>(dst_column->chunk(0));

    // both src & dst are not nullable, use raw_values
    const auto* src_raw = src_chunk->raw_values();
    const auto* dst_raw = dst_chunk->raw_values();

    #pragma omp parallel for schedule(dynamic) num_threads(num_threads)
    for(int64_t row = 0; row < src_chunk->length(); ++row) {
        auto src_id = src_prop_index_map.find(src_raw[row]);
        auto dst_id = dst_prop_index_map.find(dst_raw[row]);

        if(src_id == src_prop_index_map.end() || dst_id == dst_prop_index_map.end()) {
            logger("[WARNING] edge "+std::to_string(src_raw[row])+"->"+std::to_string(dst_raw[row])+" not found in original graph.");
            continue;
        }

        edge_translation[row] = EdgeSmall{src_id->second, dst_id->second, row};
    }
}

template <typename SrcColumnType, typename DstColumnType, typename Streamer>
std::vector<EdgeSmall> ExtractEdges(
                  Streamer& streamer,
                  const std::shared_ptr<arrow::ChunkedArray> src_column,
                  const std::shared_ptr<arrow::ChunkedArray> dst_column,
                  const std::unordered_map<int64_t, graphar::IdType>& src_prop_index_map,
                  const std::unordered_map<int64_t, graphar::IdType>& dst_prop_index_map,
                  int64_t num_of_edges_in_chunk, std::vector<EdgeSmall>& new_chunk_edges) {

    new_chunk_edges.reserve(num_of_edges_in_chunk);

    auto src_chunk = std::static_pointer_cast<SrcColumnType>(src_column->chunk(0));
    auto dst_chunk = std::static_pointer_cast<DstColumnType>(dst_column->chunk(0));

    // both src & dst are not nullable, use raw_values
    const auto* src_raw = src_chunk->raw_values();
    const auto* dst_raw = dst_chunk->raw_values();

    // TODO: while-iterator for vector & file with unified interface
    int64_t edge_idx;
    while (streamer.next(edge_idx)) {
        if (edge_idx > src_chunk->length()) {
            logger("[ERROR] index out of range: demanded "+std::to_string(edge_idx)+"'s element of user table of length "+std::to_string(src_chunk->length()));
        }
        auto val_src = src_prop_index_map.find(src_raw[edge_idx]);
        auto val_dst = dst_prop_index_map.find(dst_raw[edge_idx]);

        if (val_src == src_prop_index_map.end() || val_dst == dst_prop_index_map.end()) {
            bool src_found = (val_src != src_prop_index_map.end());
            bool dst_found = (val_dst != dst_prop_index_map.end());

            std::cout << "[WARNING] some vertices of the edge " << src_raw[edge_idx] << "->" << dst_raw[edge_idx] 
                        << " were not found in graph:" 
                        << "src: " << (src_found ? "found" : "not found, ") 
                        << "dst: " << (dst_found ? "found" : "not found.") 
                        << std::endl;
            continue;
        }

        new_chunk_edges.emplace_back(EdgeSmall{
            val_src->second,
            val_dst->second,
            edge_idx
        });
    }

    if constexpr (std::is_same_v<Streamer, FileStream>) {
        clear_file(streamer.get_path());
    }

    return new_chunk_edges;
}

template <typename ArrowArrayType>
void CollectRowNumbers(const std::shared_ptr<arrow::ChunkedArray>& column,
                      arrow::Int64Builder& pk2row,
                      std::unordered_map<int64_t, graphar::IdType>& map) {

    for (int64_t chunk_idx = 0; chunk_idx < column->num_chunks(); ++chunk_idx) {
        auto chunk = column->chunk(chunk_idx);
        auto arr = std::static_pointer_cast<ArrowArrayType>(chunk); 
        const auto* data = arr->raw_values();

        for (int64_t i = 0; i < arr->length(); ++i) {

            auto val = map.find(data[i]);
            if (val == map.end()) {
                pk2row.AppendNull();
            } else {
                pk2row.Append(val->second);
            }
        }
    }
}

void ConstructBuilderBinsearch(
    arrow::Int64Builder& builder,
    const int64_t* src_column_raw, const int64_t* dst_column_raw,
    std::vector<EdgeSmall>& new_chunk_edges,
    int64_t length) {

    for(int64_t i = 0; i < length; ++i) {
        int64_t src = src_column_raw[i];
        int64_t dst = dst_column_raw[i];

        // search for this edge
        auto it = std::lower_bound(new_chunk_edges.begin(), new_chunk_edges.end(), EdgeSmall{src, dst, -1});
        if (it != new_chunk_edges.end() && it->src == src && it->dst == dst) {
            builder.Append(it->row);
        } else {
            builder.AppendNull();
        }
    }
}

void ConstructBuilderLinear(
    arrow::Int64Builder& builder,
    const int64_t* src_column_raw, const int64_t* dst_column_raw,
    std::vector<EdgeSmall>& new_chunk_edges,
    int64_t length, bool ordered_by_src) {

    int64_t last_valid_edge = 0;
    for(int64_t i = 0; i < length; ++i) {
        int64_t src = src_column_raw[i];
        int64_t dst = dst_column_raw[i];

        // in case user gave additional edges, we must skip them
        while(last_valid_edge < new_chunk_edges.size()) { 
            if (ordered_by_src && 
                (new_chunk_edges[last_valid_edge].src < src || (new_chunk_edges[last_valid_edge].src == src && new_chunk_edges[last_valid_edge].dst < dst))
                || !ordered_by_src && 
                (new_chunk_edges[last_valid_edge].dst < dst || (new_chunk_edges[last_valid_edge].dst == dst && new_chunk_edges[last_valid_edge].src < src))) {
                ++last_valid_edge;
            } else {
                break; 
            }
        }

        // search for this edge
        if(last_valid_edge < new_chunk_edges.size() && 
            new_chunk_edges[last_valid_edge].src == src && new_chunk_edges[last_valid_edge].dst == dst) {
            builder.Append(new_chunk_edges[last_valid_edge].row);
            ++last_valid_edge;
        } else {
            builder.AppendNull();
        }
    }
}


std::string make_mapping_path(std::string user_tmp, int chunk) {
    return user_tmp + "/chunk_" + std::to_string(chunk);
}

/* Designed to write edge_to_chunk_mapping into files.
*  Files will be stored in user-specified_tmp_path/mapping directory, named 'chunk_k'.
*/
bool WriteMappingNClearVector(std::vector<std::vector<std::vector<int64_t>>>& data,
                              std::string& tmp_path, int num_threads = 1) {
    bool is_ok = true;
    int num_of_chunks = data[0].size();

    #pragma omp parallel for schedule(dynamic) num_threads(num_threads)
    for(int chunk = 0; chunk < num_of_chunks; ++chunk) {

        #pragma omp cancellation point for
        if (!is_ok) continue; 

        std::string path_to_chunk = tmp_path + "/chunk_" + std::to_string(chunk); 
        for(int t = 0; t < data.size(); ++t) {
            try {
                append_to_bin(data[t][chunk], path_to_chunk);
            } catch (const std::exception& e) {
                std::cout << "[ERROR] append_to_bin failed: " << e.what() << "\n";
                #pragma omp critical
                {
                    is_ok = false;
                }
                #pragma omp cancel for
                break;
            }
        }
    }

    if(is_ok) {
        for(int chunk = 0; chunk < num_of_chunks; ++chunk) {
            for(int t = 0; t < data.size(); ++t) {
                data[t][chunk].clear();
            }
        }
        return true;
    } else {
        clear_directory(tmp_path);
        return false;
    }
}


template <typename ArrowArrayType>
bool PreProcessArray(
    const std::shared_ptr<arrow::Array>& column,
    std::vector<std::vector<std::vector<int64_t>>>& edge_to_chunk_mapping,
    std::unordered_map<int64_t, graphar::IdType>& vertex_prop_index_map,
    int num_threads, int chunk_size, int num_of_drops, std::string path_to_tmp)
{
    auto arr = std::static_pointer_cast<ArrowArrayType>(column);
    const auto* data = arr->raw_values();
    int64_t length = arr->length();
    bool wrote_tmp_files = false;

    int64_t batch_size = length % num_of_drops == 0 ? length / num_of_drops : length / num_of_drops + 1;
    for(int64_t start = 0; start < length; start += batch_size) {
        int64_t end = std::min(start + batch_size, length);

        #pragma omp parallel for schedule(static) num_threads(num_threads)
        for (int64_t i = start; i < end; ++i) {

            int thread_id = omp_get_thread_num();
            int64_t key = static_cast<int64_t>(data[i]);

            auto val = vertex_prop_index_map.find(key);

            if (val == vertex_prop_index_map.end()) {

                #pragma omp critical
                {
                    std::cout << "[Error: mapping] Could not find object in vertex_prop_index_map, row: " << i
                            << " value: " << key
                            << " thread: " << thread_id
                            << std::endl;
                }
                continue;
            }

            edge_to_chunk_mapping[thread_id][val->second / chunk_size].push_back(i);
        }

        if(path_to_tmp != "") {
            wrote_tmp_files = WriteMappingNClearVector(edge_to_chunk_mapping, path_to_tmp, num_threads);
            if (wrote_tmp_files)
                logger("    Wrote ["+std::to_string(start)+", "+std::to_string(end)+"] mapping to '"+path_to_tmp+"'.");
            else {
                logger("    [ERROR] Could not write mapping to '"+path_to_tmp+"'.");

                // if this is the first chunk, and we have no data on disk
                if(start == 0) {
                    path_to_tmp = "";  // we will store everything in memory
                    logger("    Since writing to tmp folder failed, data will be stored in memory.");
                } else {
                    clear_directory(path_to_tmp);
                    throw std::runtime_error("Could not write files to tmp directory for batch "+std::to_string(start / batch_size + 1)+" (starting from 1). Can't recover.");
                }
            }
        }
    }

    if(path_to_tmp != "") {
        edge_to_chunk_mapping.clear();
        std::vector<std::vector<std::vector<int64_t>>>().swap(edge_to_chunk_mapping);
    }

    return wrote_tmp_files;
}


std::string DoMerge(const py::dict& config_dict)
{
    logger("Mege started");
    size_t num_threads = omp_get_max_threads();

    // getting config data
    MergeConfig merge_config;
    merge_config.fill(config_dict);
    auto graph_info = graphar::GraphInfo::Load(
            merge_config.graphar_config.path+"/"+merge_config.graphar_config.name+".yaml").value();

    // Create some usefull containers and read useful values
    fs::path save_path = merge_config.graphar_config.path;
    graphar::VertexInfoVector vertices_info;
    auto version = graphar::InfoVersion::Parse(merge_config.graphar_config.version).value();

    std::unordered_map<std::string, graphar::IdType> vertex_chunk_sizes;
    for (const auto& vertex_info : graph_info->GetVertexInfos()) {
        vertex_chunk_sizes[vertex_info->GetType()] = vertex_info->GetChunkSize();
    }

    // 0. Vertex load
    // 1. Read GraphAr Vertex info
    // 2. Modify & rewrite this vertex info
    // 3. Collect PK+index to unordered map
    // 4. For each element in new table, get internal graphAr index-> Add data to this posotion
    // 5. Save

    // 1. Add attributes to vertices
    logger("Processing vertices");
    for (const auto& vertex : merge_config.merge_schema.vertices) {

        // 1.1 Go to the graph description yml's and load information about this vertex
        logger("  Processing vertex <"+vertex.type+">.");
        auto vertex_info = graph_info->GetVertexInfo(vertex.type);

        // 1.2 Read info about property groups that will be added and add it to the current information
        // TODO: note: this looks a lot like importer.h, we probably need refactoring 
        logger("    Reading PG that should be added.");
        std::string primary_key;
        auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>(vertex_info->GetPropertyGroups());
        int number_of_pgroups = pgs.size();

        for (const auto& pg : vertex.property_groups) {
            ++number_of_pgroups;
            std::vector<graphar::Property> props;
            for (const auto& prop : pg.properties) {
                if (prop.is_primary) {
                    if (!primary_key.empty()) {
                        throw std::runtime_error("Multiple primary keys found in vertex " +
                                                vertex.type);
                    }
                    primary_key = prop.name;
                } else {
                    graphar::Property property(
                        prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
                        prop.is_primary, prop.nullable);
                    props.push_back(property);
                }
            }
            auto property_group = graphar::CreatePropertyGroup(
                props, graphar::StringToFileType(pg.file_type), 
                vertex.type+"_properties_"+std::to_string(number_of_pgroups));
            pgs.emplace_back(property_group);
        }
        logger("    Additional PG added to config.");

        // Update vertex info
        auto vertex_info_updated =
                    graphar::CreateVertexInfo(vertex.type, vertex.chunk_size, pgs,
                                  vertex.labels, vertex.prefix, version);

        auto file_name = vertex.type + ".vertex.yaml";
        auto res = vertex_info_updated->Save(save_path / file_name);
        //vertices_info.push_back(vertex_info_updated);
        logger("    Saved updated vertex description.");

        // Create vertex property writer to save new data
        auto save_path_str = save_path.string();
        save_path_str += "/";
        auto vertex_prop_writer = graphar::VertexPropertyWriter::Make(
                                    vertex_info_updated, save_path_str,
                                    StringToValidateLevel(vertex.validate_level))
                                    .value();

        // 1.3 Read graph vertices' columns with PK and graphar index
        // 1.3.1 Read graph's original PG to find user's PK there
        std::vector<std::shared_ptr<graphar::PropertyGroup>> original_pgs = vertex_info->GetPropertyGroups();
        std::shared_ptr<graphar::PropertyGroup> pg_with_user_PK;
        for(auto& pg: original_pgs) {
            for (const auto& prop : pg->GetProperties()) {
                if (prop.name == vertex.join_on) {
                    pg_with_user_PK = pg;
                    break;
                }
            }
        }
        if (pg_with_user_PK.get() == nullptr) {
            throw std::runtime_error("No property '"+vertex.join_on+"' found in original schema.");
        }
        std::string path_original = merge_config.graphar_config.path + '/' + 
                                    vertex_info->GetPathPrefix(pg_with_user_PK).value();
        logger("    Looking for original data in "+path_original);

        // 1.3.3 Read new data
        std::vector<std::shared_ptr<arrow::Table>> vertex_tables;
        for(const Source& source : vertex.sources) {
            // Read source's column names
            std::vector<std::string> new_column_names;
            for (const auto& [key, value] : source.columns) {
                new_column_names.emplace_back(key);
            }

            // Read source
            std::shared_ptr<arrow::Table> table;
            {
                std::vector<std::shared_ptr<arrow::Table>> file_tables(source.path.size());
                for (int i = 0; i < source.path.size(); ++i) {
                    file_tables[i] = GetDataFromFile(source.path[i], new_column_names, source.delimiter,
                                        source.file_type);
                }
                table = ConcatenateTables(file_tables).ValueOrDie();
            }

             // Change name and datatype step
            std::unordered_map<std::string, Property> column_prop_map;
            std::unordered_map<std::string, std::string> reversed_columns_config;
            for (const auto& [key, value] : source.columns) {
                reversed_columns_config[value] = key;
            }
            for (const auto& pg : vertex.property_groups) {
                for (const auto& prop : pg.properties) {
                    column_prop_map[reversed_columns_config[prop.name]] = prop;
                }
            }
            std::unordered_map<
                std::string, std::pair<std::string, std::shared_ptr<arrow::DataType>>>
                columns_to_change;
            for (const auto& [column, prop] : column_prop_map) {
                auto arrow_data_type = graphar::DataType::DataTypeToArrowDataType(
                    graphar::DataType::TypeNameToDataType(prop.data_type));
                auto arrow_column = table->GetColumnByName(column);

                if (!prop.nullable) {
                    for (const auto& chunk : arrow_column->chunks()) {
                        if (chunk->null_count() > 0) {
                        throw std::runtime_error("Non-nullable column '" + column +
                                                "' has null values");
                        }
                    }
                }
                if (column != prop.name || arrow_column->type()->id() != arrow_data_type->id()) {
                    columns_to_change[column] = std::make_pair(prop.name, arrow_data_type);
                }
            }
            table = ChangeNameAndDataType(table, columns_to_change);
            vertex_tables.push_back(table);
        }
                
        // Merge all tables with new data into a big one
        std::shared_ptr<arrow::Table> merged_vertex_table = MergeTables(vertex_tables);

        // 1.3.4 Save map[user_pk] = row-number-in-input-table
        // note: only int64/int32 keys are allowed
        logger("    Mapping PK from new data to its row in new data."); // TODO: OMP (22-01-01 6 mins one thread) ??? 
        std::unordered_map<int64_t, graphar::IdType> pk2row_num;
        auto pk_column = merged_vertex_table->GetColumnByName(vertex.join_on);
        if (pk_column->null_count() > 0) {
            throw std::runtime_error("Vertex PK property column '" + vertex.join_on + "' has NULL values.");
        }

        switch (pk_column->chunk(0)->type_id()) {
            case arrow::Type::INT32:
                MapPK2row<arrow::Int32Array>(pk_column, pk2row_num);
                break;
            case arrow::Type::INT64:
                MapPK2row<arrow::Int64Array>(pk_column, pk2row_num);
                break;
            default:
                throw std::runtime_error("Unsupported type of PK in user files.");
        }

        // 1.3.5 For each chunk in GraphAr collect rows in additional data that match it
        std::vector<std::string> column_names = {vertex.join_on};

        std::vector<std::filesystem::directory_entry> parts;
        for (auto& p : std::filesystem::directory_iterator(path_original)) {
            parts.push_back(p);
        }

        logger("      Merging data to vertex chunks.");
        #pragma omp parallel for schedule(dynamic) num_threads(std::min(num_threads, parts.size()))
        for (int64_t i = 0; i < parts.size(); ++i) {
            auto& file = parts[i];

            // read one vertex chunk in GraphAr format
            std::shared_ptr<arrow::ChunkedArray> vertex_chunk_column = 
                            GetDataFromParquetFile(file.path().string(), column_names)->column(0);
            arrow::Int64Builder builder;
            std::optional<int> vertex_chunk_idx = extract_tailing_number(file);
            if(!vertex_chunk_idx.has_value()) {  // in case we change format and other files will appear in the same directory
                logger("  [WARNING] Found file with no tailing number in graph's vertex.");
                continue;
            }

            // for each PK find the corresponding line number in additional attributes
            switch(vertex_chunk_column->chunk(0)->type_id()) {
                case arrow::Type::INT32:
                    CollectRowNumbers<arrow::Int32Array>(vertex_chunk_column, builder, pk2row_num);
                    break;
                case arrow::Type::INT64:
                    CollectRowNumbers<arrow::Int64Array>(vertex_chunk_column, builder, pk2row_num);
                    break;
                default:
                    throw std::runtime_error("Unsupported type of PK in provided GraphAr data.");
            }

            // collect the result
            std::shared_ptr<arrow::Array> indices_order;
            builder.Finish(&indices_order);

            // exctract data in correct order
            arrow::compute::TakeOptions options;
            auto maybe_sorted_chunk = arrow::compute::Take(merged_vertex_table, indices_order, options);
            auto sorted_chunk = maybe_sorted_chunk.ValueOrDie().table();

            // Write table
            for (const auto& property_group : pgs) {
                vertex_prop_writer->WriteTable(sorted_chunk, property_group,
                                                vertex_chunk_idx.value());
            }
        }
        logger("  Processed vertex <"+vertex.type+">.");  // TODO: pause for 1.5 minutes, why?
    }

    // 2. Add attributes to edges
    logger("Processing edges.");

    // 2.1. We should know graphar ids of vertices to which we refer in edges
    std::map<std::pair<std::string, std::string>, 
           std::unordered_map<int64_t, graphar::IdType>> vertex_prop_index_map;
    std::unordered_map<std::string, std::set<std::string>>
      vertex_props_in_edges;

    // 2.1.1 Collect types of vertices connected by each type of edge
    //       and properties to which edges refer.
    for (const auto& edge : merge_config.merge_schema.edges) {
        vertex_props_in_edges[edge.src_type].insert(edge.src_prop);
        vertex_props_in_edges[edge.dst_type].insert(edge.dst_prop);
    }

    // 2.1.2 For each vertex type used in edges, find properties which
    //       edges refer to, read property & id columns and save property->id
    //       relation in the unordered_map.
    for(auto vertex : graph_info->GetVertexInfos()) {
        if (vertex_props_in_edges.find(vertex->GetType()) == vertex_props_in_edges.end()) 
            continue;
        
        for (const auto& vertex_prop : vertex_props_in_edges[vertex->GetType()]) {
            if (vertex_prop_index_map.find(std::make_pair(vertex->GetType(), vertex_prop)) != vertex_prop_index_map.end())
                continue;
            
            // find PG that contains this property
            std::optional<std::string> path_to_pg;
            for(auto& pg : vertex->GetPropertyGroups()) {
                if (pg->HasProperty(vertex_prop)) {
                    path_to_pg = pg->GetPrefix();
                    break;
                }
            } 
            if(!path_to_pg.has_value()) {
                throw std::runtime_error("No vertex property "+vertex_prop+" found in graph.");
            }
            
            std::string path_to_graphar_pg = merge_config.graphar_config.path + '/' + 
                                                vertex->GetPrefix() + path_to_pg.value();
            logger("  Looking for property '"+ vertex_prop + "' in " + path_to_graphar_pg);

            int64_t vertex_num = 0;
            {
                std::string path_to_vertex_count = merge_config.graphar_config.path + '/' + vertex->GetVerticesNumFilePath().value();
                logger("  Path to vertex num: "+path_to_vertex_count);
                std::ifstream file(path_to_vertex_count, std::ios::binary);
                file.read(reinterpret_cast<char*>(&vertex_num), sizeof(vertex_num));
                vertex_num = le64toh(vertex_num);  // TODO: important: make it correct for writer  
            }

            // read tables from directory and save property_value -> vertex_id relation
            std::unordered_map<int64_t, graphar::IdType> property_to_id_map(vertex_num);
            std::vector<std::string> column_names = {vertex_prop, graphar::GeneralParams::kVertexIndexCol};
            
            for (const auto& file : std::filesystem::directory_iterator(path_to_graphar_pg)) {  // TODO: omp (8 minutes on 22-01-01)
                std::shared_ptr<arrow::Table> vertex_chunk_prop_columns =                    
                                GetDataFromParquetFile(file.path().string(), column_names);
                switch(vertex_chunk_prop_columns->GetColumnByName(vertex_prop)->chunk(0)->type_id()) {
                    case arrow::Type::INT32:
                        MapValues<arrow::Int32Array, arrow::Int64Array>(vertex_prop, graphar::GeneralParams::kVertexIndexCol, 
                                                                        vertex_chunk_prop_columns, property_to_id_map);
                        break;
                    case arrow::Type::INT64:
                        MapValues<arrow::Int64Array, arrow::Int64Array>(vertex_prop, graphar::GeneralParams::kVertexIndexCol, 
                                                                        vertex_chunk_prop_columns, property_to_id_map);
                        break;
                    default:
                        throw std::runtime_error("Unsupported type of PK in provided GraphAr data.");
                }
            }
            logger("  Property '" + vertex_prop + "' mapping to GraphAr id saved.");   // TODO: pause for 6 mins after that, why?
            // save map for future usage
            vertex_prop_index_map[std::make_pair(vertex->GetType(), vertex_prop)] = property_to_id_map;
        }
    }

    // 2.2 Work with one edge at a time
    for (const auto& edge : merge_config.merge_schema.edges) {
        logger("  Processing edge <"+edge.edge_type+">.");

        // 2.2.0 Create edge_info & edge_writer for this edge
        auto edge_info = graph_info->GetEdgeInfo(edge.src_type, edge.edge_type, edge.dst_type);

        // collect all pgs that will be added to this edge
        auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>(edge_info->GetPropertyGroups());
        int number_of_pgroups = pgs.size();

        for (const auto& pg : edge.property_groups) {
            ++number_of_pgroups;
            std::vector<graphar::Property> props;
            for (const auto& prop : pg.properties) {
                graphar::Property property(
                    prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
                    prop.is_primary, prop.nullable);
                props.push_back(property);
            }
            auto property_group = graphar::CreatePropertyGroup(
                props, graphar::StringToFileType(pg.file_type), 
                edge.edge_type+"_properties_"+std::to_string(number_of_pgroups));
            pgs.emplace_back(property_group);
        }

        // collect adj lists info
        graphar::AdjacentListVector original_adj_lists;
        for (const auto& adj_list : edge.adj_lists) {
            original_adj_lists.push_back(graphar::CreateAdjacentList(
                                            graphar::OrderedAlignedToAdjListType(adj_list.ordered,
                                                                                adj_list.aligned_by),
                                            graphar::StringToFileType(adj_list.file_type)));
        }

        // update edge info
        auto updated_edge_info = graphar::CreateEdgeInfo(
            edge.src_type, edge.edge_type, edge.dst_type, edge.chunk_size,
            vertex_chunk_sizes[edge.src_type], vertex_chunk_sizes[edge.dst_type],
            true, original_adj_lists, pgs, edge.prefix, version);

        // Work with one new PG at a time
        // TODO: additional properties already exist -> overwrite them, add overwrite flg in config ???
        for(auto& pg : edge.property_groups) {

            // 2.2.1 Define which source has this PG data
            std::optional<Source> source_PG;
            for (const auto& source : edge.sources) {

                // collect properties that are defined in this source 
                std::vector<std::string> prop_names_in_source;
                prop_names_in_source.reserve(source.columns.size());
                for (const auto& [data_column_name, prop_name] : source.columns) {
                    prop_names_in_source.push_back(prop_name);
                }

                // make sure all properties are in this source
                bool all_props_in_source = true;
                for(const auto& prop : pg.properties) {
                    auto it = std::find(prop_names_in_source.begin(), 
                                        prop_names_in_source.end(), 
                                        prop.name);
                    if (it == prop_names_in_source.end()) {
                        all_props_in_source = false;
                        break;
                    }
                }

                if(all_props_in_source) {
                    source_PG = source;
                    break;
                }
            }
            if(!source_PG.has_value()) {
                throw std::runtime_error("There is no source that contains all properties from this PG.");
            }

            // 2.2.2 Read source table with new PG
            std::vector<std::string> pg_column_names;
                for (const auto& [key, value] : source_PG.value().columns) {
                pg_column_names.emplace_back(key);
            }

            std::shared_ptr<arrow::Table> pg_data_table;
            {
                std::vector<std::shared_ptr<arrow::Table>> file_tables(source_PG.value().path.size());

                #pragma omp parallel for schedule(dynamic) num_threads(std::min(num_threads, source_PG.value().path.size()))
                for (int i = 0; i < source_PG.value().path.size(); ++i) {
                    file_tables[i] = GetDataFromFile(source_PG.value().path[i], pg_column_names,
                                                    source_PG.value().delimiter, source_PG.value().file_type);
                }
                logger("[DEBUG] before ConcatenateTables()");
                auto pg_data_table_tmp = ConcatenateTables(file_tables).ValueOrDie(); 
                pg_data_table = pg_data_table_tmp;
                logger("    PG source read: "+std::to_string(source_PG.value().path.size()) +" tables concatenated.");
            }  
            
            // Change name and data type step
            std::unordered_map<std::string, graphar::Property> column_prop_map;
            std::unordered_map<std::string, std::string> reversed_columns;
            for (const auto& [key, value] : source_PG.value().columns) {
                reversed_columns[value] = key;
            }

            for (const auto& pg : edge.property_groups) {
                for (const auto& prop : pg.properties) {
                    column_prop_map[reversed_columns[prop.name]] = graphar::Property(
                        prop.name,
                        graphar::DataType::TypeNameToDataType(prop.data_type),
                        prop.is_primary, prop.nullable);
                }
            }
            std::unordered_map<
                std::string,
                std::pair<std::string, std::shared_ptr<arrow::DataType>>>
                columns_to_change;

            for (const auto& [column, prop] : column_prop_map) {
                auto arrow_data_type =
                    graphar::DataType::DataTypeToArrowDataType(prop.type);
                auto arrow_column = pg_data_table->GetColumnByName(column);
                if (!prop.is_nullable) {
                    for (const auto& chunk : arrow_column->chunks()) {
                        if (chunk->null_count() > 0) {
                            throw std::runtime_error("Non-nullable column '" + column +
                                                    "' has null values");
                        }
                    }
                }
                if (column != prop.name || arrow_column->type()->id() != arrow_data_type->id()) {
                    columns_to_change[column] = std::make_pair(prop.name, arrow_data_type);
                }
            }
            logger("[DEBUG] Before ChangeNameAndDataType().");
            pg_data_table = ChangeNameAndDataType(pg_data_table, columns_to_change);
            logger("    Name & data type changed, columns to change: "+std::to_string(columns_to_change.size()));
            pg_data_table = pg_data_table->CombineChunks().ValueOrDie();
            logger("[DEBUG] after CombineChunks()");

            // 2.2.3 For each row define src&dst graphar ids, remember the row with data.
            
            //       Get columns with src&dst
            std::shared_ptr<arrow::ChunkedArray> src_column_tmp = pg_data_table->GetColumnByName(reversed_columns[edge.src_edge_prop]);
            std::shared_ptr<arrow::ChunkedArray> dst_column_tmp = pg_data_table->GetColumnByName(reversed_columns[edge.dst_edge_prop]);

            for(auto column_to_remove: std::vector<std::string>{reversed_columns[edge.src_edge_prop], reversed_columns[edge.dst_edge_prop]}) {
                
                logger("    Removing column " + column_to_remove);
                int remove_idx = pg_data_table->schema()->GetFieldIndex(column_to_remove);
                pg_data_table = pg_data_table->RemoveColumn(remove_idx).ValueOrDie();
            }

            auto result = arrow::Concatenate(src_column_tmp->chunks());
            if (!result.ok()) {
                std::cerr << result.status().ToString() << std::endl;
                throw std::runtime_error("Could not combine chunks for PK column 1.");
            }
            auto combined_src_array = result.ValueOrDie();

            result = arrow::Concatenate(dst_column_tmp->chunks());
            if (!result.ok()) {
                std::cerr << result.status().ToString() << std::endl;
                throw std::runtime_error("Could not combine chunks for PK column 2.");
            }
            auto combined_dst_array = result.ValueOrDie();

            auto src_column = std::make_shared<arrow::ChunkedArray>(combined_src_array);
            auto dst_column = std::make_shared<arrow::ChunkedArray>(combined_dst_array);

            arrow::Type::type src_prop_type = src_column->chunk(0)->type_id();
            arrow::Type::type dst_prop_type = dst_column->chunk(0)->type_id();
            if (src_column->null_count() > 0) {
                throw std::runtime_error("Edge src PK property column '" + edge.src_edge_prop + "' has NULL values.");
            }
            if (dst_column->null_count() > 0) {
                throw std::runtime_error("Edge src PK property column '" + edge.dst_edge_prop + "' has NULL values.");
            }

            // 2.2.4 Work with each adj_lists type required by user
            for (const auto& adj_list : edge.adj_lists) {  // TODO: user demands adj_list that does not exist in original graph
                logger("    Working with adj_list aligned by "+adj_list.aligned_by);
                num_threads = omp_get_max_threads();

                auto adj_lst = graphar::CreateAdjacentList(
                                    graphar::OrderedAlignedToAdjListType(adj_list.ordered,
                                                                        adj_list.aligned_by),
                                    graphar::StringToFileType(adj_list.file_type)
                );

                // create writer for this edge & adj list type
                graphar::EdgeChunkWriter edge_writer(updated_edge_info, save_path.string()+"/", adj_lst->GetType(), 
                                                     graphar::WriterOptions::DefaultWriterOption(),
                                                     StringToValidateLevel(edge.validate_level));
                
                // calculate number of chunks according to the number of src/dst vertices
                int num_of_chunks = 0;
                int64_t vertex_chunk_size = 0;
                bool aligned_by_src = true;

                if(adj_lst->GetType() == graphar::AdjListType::ordered_by_source ||
                   adj_lst->GetType() == graphar::AdjListType::unordered_by_source) {
                    vertex_chunk_size = vertex_chunk_sizes[edge.src_type];
                    num_of_chunks = vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)).size() / 
                                    vertex_chunk_size + 1;
                } else {
                    aligned_by_src = false;
                    vertex_chunk_size = vertex_chunk_sizes[edge.dst_type];
                    num_of_chunks = vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)).size() / 
                                    vertex_chunk_size + 1;
                }

                // map edge row to its chunk TODO reserve
                std::vector<std::vector<std::vector<int64_t>>> edge_to_chunk_mapping(
                    num_threads,
                    std::vector<std::vector<int64_t>>(num_of_chunks)
                );
                for (int t = 0; t < num_threads; ++t) {
                    for (int c = 0; c < num_of_chunks; ++c) {
                        edge_to_chunk_mapping[t][c].reserve(graph_info->GetEdgeInfos()[0]->GetChunkSize() * 5 / num_threads);  
                    } // WARNING: depends on the graph, better choose constant manually for each launch
                }

                // use importer approach
                int num_of_drops = 2;
                bool wrote_tmp_files = false;
                logger("    Mapping edge row to its chunk in "+std::to_string(num_threads)+" threads.");
                if (adj_lst->GetType() == graphar::AdjListType::ordered_by_source ||
                    adj_lst->GetType() == graphar::AdjListType::unordered_by_source)
                {
                    if (src_prop_type == arrow::Type::INT64) {
                        wrote_tmp_files = PreProcessArray<arrow::Int64Array>(
                                            src_column->chunk(0), edge_to_chunk_mapping,
                                            vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)), 
                                            num_threads, edge_info->GetSrcChunkSize(), num_of_drops, merge_config.tmp_path);
                    }
                    else if (src_prop_type == arrow::Type::INT32) {
                        wrote_tmp_files = PreProcessArray<arrow::Int32Array>(
                                            src_column->chunk(0), edge_to_chunk_mapping,
                                            vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)), 
                                            num_threads, edge_info->GetDstChunkSize(), num_of_drops, merge_config.tmp_path);
                    }
                    else {
                        throw std::runtime_error("Unsupported type");
                    }
                } else {
                    if (dst_prop_type == arrow::Type::INT64) {
                        wrote_tmp_files = PreProcessArray<arrow::Int64Array>(
                                            dst_column->chunk(0), edge_to_chunk_mapping,
                                            vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)), 
                                            num_threads, edge_info->GetSrcChunkSize(), num_of_drops, merge_config.tmp_path);
                    }
                    else if (dst_prop_type == arrow::Type::INT32) {
                        wrote_tmp_files = PreProcessArray<arrow::Int32Array>(
                                            dst_column->chunk(0), edge_to_chunk_mapping,
                                            vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)), 
                                            num_threads, edge_info->GetDstChunkSize(), num_of_drops, merge_config.tmp_path);
                    }
                    else {
                        throw std::runtime_error("Unsupported type");
                    }
                }
                logger("    Mapping complete.");

                // Edges are sorted by their chunks, we only need to:
                // 1) Sort them the same way as in the original adj_lists (read only one edge chunk for that).
                // 2) Create table by extracting values from the saved rows in correct order.
                // 3) Save table to a specific directory.

                // 2.2.5 Sort edges the same way they are sorted in chunks
                std::string path_to_adjlist = merge_config.graphar_config.path + '/' 
                                              + edge.prefix + adj_lst->GetPrefix()+"adj_list";
                logger("    Looking for original data in "+path_to_adjlist);

                std::vector<std::filesystem::directory_entry> parts;
                for (auto& p : std::filesystem::directory_iterator(path_to_adjlist)) {
                    parts.push_back(p);
                }

                std::vector<std::string> column_names = {graphar::GeneralParams::kSrcIndexCol, graphar::GeneralParams::kDstIndexCol};
                num_threads = omp_get_max_threads() / 4;
                //num_threads = 2;
                int processed_chunks = 0;
                logger("    Building edges in " + std::to_string(num_threads) + " threads.");
                #pragma omp parallel for schedule(dynamic) num_threads(std::min(num_threads, parts.size()))
                for (int64_t i = 0; i < parts.size(); ++i) {

                    auto& edge_chunk_path = parts[i];
                    std::optional<int> edge_chunk_idx = extract_tailing_number(edge_chunk_path);
                    if(!edge_chunk_idx.has_value()) {
                        logger("  [WARNING] Found edge chunk with no tailing number.");
                        continue;
                    }
                    std::string path_to_mapping = make_mapping_path(merge_config.tmp_path, edge_chunk_idx.value());

                    // calculate number of edges
                    int64_t num_of_edges_in_chunk = 0;
                    if(!wrote_tmp_files) {
                        for(int thread = 0; thread < edge_to_chunk_mapping.size(); ++thread) {
                            num_of_edges_in_chunk += edge_to_chunk_mapping[thread][edge_chunk_idx.value()].size();
                        }
                    } else {
                        num_of_edges_in_chunk = get_count_bin(path_to_mapping);
                    }

                    // collect edges by edge_chunk_idx, sort them
                    std::vector<EdgeSmall> new_chunk_edges;
                    if (src_prop_type == arrow::Type::INT64 && dst_prop_type == arrow::Type::INT64)
                        with_streamer(wrote_tmp_files, edge_to_chunk_mapping, path_to_mapping, edge_chunk_idx.value(),
                        [&](auto& s) {
                            ExtractEdges<arrow::Int64Array, arrow::Int64Array>(
                                s, src_column, dst_column, 
                                vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)),
                                vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)),
                                num_of_edges_in_chunk, new_chunk_edges
                            );
                        });
                    else if (src_prop_type == arrow::Type::INT64 && dst_prop_type == arrow::Type::INT32)
                        with_streamer(wrote_tmp_files, edge_to_chunk_mapping, path_to_mapping, edge_chunk_idx.value(),
                        [&](auto& s) {
                            ExtractEdges<arrow::Int32Array, arrow::Int64Array>(
                                s, src_column, dst_column, 
                                vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)),
                                vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)),
                                num_of_edges_in_chunk, new_chunk_edges
                            );
                        });
                    else if (src_prop_type == arrow::Type::INT32 && dst_prop_type == arrow::Type::INT64)
                        with_streamer(wrote_tmp_files, edge_to_chunk_mapping, path_to_mapping, edge_chunk_idx.value(),
                        [&](auto& s) {
                            ExtractEdges<arrow::Int64Array, arrow::Int32Array>(
                                s, src_column, dst_column, 
                                vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)),
                                vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)),
                                num_of_edges_in_chunk, new_chunk_edges
                            );
                        });
                    else if (src_prop_type == arrow::Type::INT32 && dst_prop_type == arrow::Type::INT32)
                        with_streamer(wrote_tmp_files, edge_to_chunk_mapping, path_to_mapping, edge_chunk_idx.value(),
                        [&](auto& s) {
                            ExtractEdges<arrow::Int32Array, arrow::Int32Array>(
                                s, src_column, dst_column, 
                                vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)),
                                vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)),
                                num_of_edges_in_chunk, new_chunk_edges
                            );
                        });
                    else
                        throw std::runtime_error("Unsupported type combination");

                    // Case 1: ordered by src/dst -> we can use linear search, if we sort edges by src/dst
                    // Case 2: unordered -> we must use binsearch, so sort everything by src
                    if (adj_lst->GetType() != graphar::AdjListType::ordered_by_dest) {
                        std::sort(new_chunk_edges.begin(), new_chunk_edges.end(),
                              [](const EdgeSmall& a, const EdgeSmall& b){return a.src == b.src ? a.dst < b.dst : a.src < b.src;});
                    } else {
                        std::sort(new_chunk_edges.begin(), new_chunk_edges.end(),
                              [](const EdgeSmall& a, const EdgeSmall& b){return a.dst == b.dst ? a.src < b.src : a.dst < b.dst;});
                    }
 
                    // read one edge chunk and make builder for it
                    for (const auto& chunk : std::filesystem::directory_iterator(edge_chunk_path.path())) {
                        arrow::Int64Builder builder;
                        std::shared_ptr<arrow::Int64Array> src_column, dst_column;
                        {
                            std::shared_ptr<arrow::Table> tmp_table = GetDataFromParquetFile(chunk.path().string(), column_names)
                                                                      ->CombineChunks().ValueOrDie();
                            src_column = std::static_pointer_cast<arrow::Int64Array>(tmp_table->GetColumnByName(column_names[0])->chunk(0));
                            dst_column = std::static_pointer_cast<arrow::Int64Array>(tmp_table->GetColumnByName(column_names[1])->chunk(0));
                        }

                        // for each edge find reference to its additional properties
                        const auto* src_column_raw = src_column->raw_values();
                        const auto* dst_column_raw = dst_column->raw_values();

                        // construct builder
                        if (adj_lst->GetType() == graphar::AdjListType::unordered_by_dest || adj_lst->GetType() == graphar::AdjListType::unordered_by_source) {
                            // when adj_lists are unordered, we will have to do binsearch on ordered by src user edges for each adj_lists edge
                            ConstructBuilderBinsearch(builder, src_column_raw, dst_column_raw, new_chunk_edges, src_column->length());
                        } else {
                            // when adj_lists are ordered by src/dst, we can order our edges by src/dst and 'merge' them for O(n)
                            ConstructBuilderLinear(builder, src_column_raw, dst_column_raw, new_chunk_edges, src_column->length(), 
                                                    adj_lst->GetType() == graphar::AdjListType::ordered_by_source ? true : false);
                        }

                        src_column.reset();
                        dst_column.reset();

                        // save order of data for this edge chunk
                        std::shared_ptr<arrow::Array> indices_order;
                        builder.Finish(&indices_order);

                        // exctract edges in correct order
                        // we extract all data, but write only properties in edge order, this is why it works
                        // we never replace PKs in new data with indices that we caclulated, bc we already have adj_lists 
                        arrow::compute::TakeOptions options = arrow::compute::TakeOptions::NoBoundsCheck();
                        auto sorted_chunk = arrow::compute::Take(pg_data_table, indices_order, options).ValueOrDie().table();

                        // write them down
                        std::optional<int> chunk_tailing_number = extract_tailing_number(chunk);
                        if (!chunk_tailing_number.has_value()) {
                            logger("  [WARNING] Found file in edge chunk with no tailing number.");
                            continue;
                        }
                        auto status = edge_writer.WritePropertyChunk(sorted_chunk, updated_edge_info->GetPropertyGroup(pg.properties[0].name), 
                                                                     edge_chunk_idx.value(), chunk_tailing_number.value(), 
                                                                     StringToValidateLevel(edge.validate_level));

                        if(!status.ok()) {
                            logger("[ERROR] Could not write chunk: " + status.message());
                        } 
                    }

                    #pragma omp critical
                    {
                        processed_chunks += 1;
                        logger("      Processed "+std::to_string(processed_chunks)+"/" + std::to_string(parts.size()) + " edge chunks.");
                    }
                } 
            }
        }

        // write new edge description
        auto file_name = edge.src_type + "_" + edge.edge_type + "_" + edge.dst_type + ".edge.yaml";
        auto status = updated_edge_info->Save(save_path / file_name);
        if(!status.ok()) {
            logger("[ERROR] Could not write edge description file: " + status.message());
        }
    }

    return "Merged successfully!";
}
