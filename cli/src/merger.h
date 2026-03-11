#pragma once

#include <iostream>
#include <filesystem>
#include <string>
#include <pybind11/pybind11.h>
#include "pybind11/stl.h"
#include "util.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

namespace py = pybind11;

int extract_tailing_number(const std::filesystem::path& filename) {
    std::string name = filename.stem().string();

    int end = name.size() - 1;
    while (end >= 0 && std::isdigit(static_cast<unsigned char>(name[end]))) {
        end--;
    }

    if (end == static_cast<int>(name.size()) - 1)
        return -1;  // chunks have only positive numbers

    std::string number = name.substr(end + 1);
    return std::stoi(number);
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
            map[static_cast<int64_t>(data[i])] = row_offset + i;
        }
        row_offset += arr->length();
    }
}

template <typename ArrowArrayType>
void CollectRowNumers(const std::shared_ptr<arrow::ChunkedArray>& column,
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

std::string DoMerge(const py::dict& config_dict)
{
    logger("Mege started");

    // getting config data
    MergeConfig merge_config;
    merge_config.fill(config_dict);
    auto graph_info = graphar::GraphInfo::Load(
            merge_config.graphar_config.path+"/"+merge_config.graphar_config.name+".yaml").value();

    // Create some usefull containers and read useful values
    fs::path save_path = merge_config.graphar_config.path;
    graphar::VertexInfoVector vertices_info;
    auto version = graphar::InfoVersion::Parse(merge_config.graphar_config.version).value();

    // 0. Vertex load
    // 1. Read GraphAr Vertex info
    // 2. Modify & rewrite this vertex info
    // 3. Collect PK+index to unordered map
    // 4. For each element in new table, get internal graphAr index-> Add data to this posotion
    // 5. Dump

    // 1. Add attributes to vertices
    logger("Processing vertices");
    for (const auto& vertex : merge_config.merge_schema.vertices) {

        // 1.1 Go to the graph description yml's and load information about this vertex
        logger("Processing vertex <"+vertex.type+">.");
        auto vertex_info = graph_info->GetVertexInfo(vertex.type);

        // 1.2 Read info about property groups that will be added and add to the current information
        // TODO: note: this looks a lot like importer.h, we probably need refactoring 
        logger("  Reading PG that should be added.");
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
        logger("  Additional PG added to config.");

        // Update vertex info
        auto vertex_info_updated =
                    graphar::CreateVertexInfo(vertex.type, vertex.chunk_size, pgs,
                                  vertex.labels, vertex.prefix, version);

        auto file_name = vertex.type + ".vertex.yaml";
        auto res = vertex_info_updated->Save(save_path / file_name);
        vertices_info.push_back(vertex_info_updated);
        logger("  Saved updated vertex description.");

        // Create vertex property writer to save new data
        auto save_path_str = save_path.string();
        save_path_str += "/";
        auto vertex_prop_writer = graphar::VertexPropertyWriter::Make(
                                    vertex_info_updated, save_path_str,
                                    StringToValidateLevel(vertex.validate_level))
                                    .value();

        // 1.3 Read graph's vertices' columns with PK and graphar index
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
        logger("  Looking for original data in "+path_original);

        // 1.3.3 Save map[user_pk] = graphar_index
        // note: only int64 keys are alowed
        // TODO: check key is int in config
        /*std::unordered_map<int64_t, graphar::IdType> pk2index = TableToUnorderedMapInt64(
                    table, vertex.join_on, graphar::GeneralParams::kVertexIndexCol
                );
        logger("  Map created.");*/

        // 1.3.3 Read new data
        std::vector<std::shared_ptr<arrow::Table>> vertex_tables;
        for(Source source : vertex.sources) {
            // Read source's column names
            std::vector<std::string> new_column_names;
            for (const auto& [key, value] : source.columns) {
                new_column_names.emplace_back(key);
            }

            // Read source
            {
                std::vector<std::shared_ptr<arrow::Table>> file_tables(source.path.size());
                for (int i = 0; i < source.path.size(); ++i) {
                    file_tables[i] = GetDataFromFile(source.path[i], new_column_names, source.delimiter,
                                        source.file_type);
                }
                std::shared_ptr<arrow::Table> table = ConcatenateTables(file_tables).ValueOrDie();
                vertex_tables.push_back(table);
            }
        }
        // Merge all tables with new data into a big one
        std::shared_ptr<arrow::Table> merged_vertex_table = MergeTables(vertex_tables);

        // 1.3.4 Save map[user_pk] = row-number-in-input-table
        // note: only int64/int32 keys are allowed
        // TODO: check key is int in config
        logger("  Mapping PK from new data to its row in new data.");
        std::unordered_map<int64_t, graphar::IdType> pk2row_num;
        auto pk_column = merged_vertex_table->GetColumnByName(vertex.join_on);
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

        for (const auto& file : std::filesystem::directory_iterator(path_original)) {
            // read one vertex chunk in GraphAr format
            std::shared_ptr<arrow::ChunkedArray> vertex_chunk_column = 
                            GetDataFromParquetFile(file.path().string(), column_names)->column(0);
            arrow::Int64Builder builder;
            int vertex_chunk_idx = extract_tailing_number(file);
            logger("    Merging data to vertex chunk "+std::to_string(vertex_chunk_idx));

            // for each PK find the corresponding line number in additional attributes
            switch(vertex_chunk_column->chunk(0)->type_id()) {
                case arrow::Type::INT32:
                    CollectRowNumers<arrow::Int32Array>(vertex_chunk_column, builder, pk2row_num);
                    break;
                case arrow::Type::INT64:
                    CollectRowNumers<arrow::Int64Array>(vertex_chunk_column, builder, pk2row_num);
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
                                                vertex_chunk_idx);
            }
        }
    }

    return "Merged successfully!";
}

