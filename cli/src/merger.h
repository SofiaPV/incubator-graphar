#pragma once

#include <iostream>
#include <filesystem>
#include <string>
#include <set>
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
        const auto* data = arr->raw_values();  // TODO: use Value()

        for (int64_t i = 0; i < arr->length(); ++i) {
            map[static_cast<int64_t>(data[i])] = row_offset + i;
        }
        row_offset += arr->length();
    }
}


// TODO: remake, take table, key-value columns, map
/* 
* Function suggests that CombineChunks() was already performed for the input table.
*/
template <typename KeyColumnType, typename ValueColumnType>
void MapValues(const std::string& key_column_name,
               const std::string& value_column_name,
               const std::shared_ptr<arrow::Table> input_table,
               std::unordered_map<int64_t, graphar::IdType>& map) {

    std::shared_ptr<arrow::Table> table = input_table->CombineChunks().ValueOrDie();

    auto key_col_ptr = table->GetColumnByName(key_column_name);
    auto val_col_ptr = table->GetColumnByName(value_column_name);

    if (!key_col_ptr || !val_col_ptr) {
        throw std::runtime_error("MapValues(): One of the columns not found in table");
    }

    auto key_chunk = std::static_pointer_cast<KeyColumnType>(key_col_ptr->chunk(0));
    auto val_chunk = std::static_pointer_cast<ValueColumnType>(val_col_ptr->chunk(0));

    if (key_chunk->length() != val_chunk->length()) {
        throw std::runtime_error("MapValues(): Key and value columns lengths do not match");
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
        logger("  Processing vertex <"+vertex.type+">.");
        auto vertex_info = graph_info->GetVertexInfo(vertex.type);

        // 1.2 Read info about property groups that will be added and add to the current information
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
        vertices_info.push_back(vertex_info_updated);
        logger("    Saved updated vertex description.");

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
        logger("    Looking for original data in "+path_original);

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
        logger("    Mapping PK from new data to its row in new data.");
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
            logger("      Merging data to vertex chunk "+std::to_string(vertex_chunk_idx));

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
        logger("  Processed vertex <"+vertex.type+">.");
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
    for(auto vertex : vertices_info) {
        if (vertex_props_in_edges.find(vertex->GetType()) == vertex_props_in_edges.end()) 
            continue;
        
        for (const auto& vertex_prop : vertex_props_in_edges[vertex->GetType()]) {
            if (vertex_prop_index_map.find(std::make_pair(vertex->GetType(), vertex_prop)) != vertex_prop_index_map.end())
                continue;
            
            // find PG that contains this property
            std::string path_to_pg;
            for(auto& pg : vertex->GetPropertyGroups()) {
                if (pg->HasProperty(vertex_prop)) {
                    path_to_pg = pg->GetPrefix();
                }
            }
            
            std::string path_to_graphar_pg = merge_config.graphar_config.path + '/' + 
                                                vertex->GetPrefix() + '/' + path_to_pg;
            logger("  Looking for property '"+ vertex_prop + "' in " + path_to_graphar_pg);
            
            // read tables from directory and save property_value -> vertex_id relation
            std::unordered_map<int64_t, graphar::IdType> property_to_id_map;  // TODO: reserve
            std::vector<std::string> column_names = {vertex_prop, graphar::GeneralParams::kVertexIndexCol};
            for (const auto& file : std::filesystem::directory_iterator(path_to_graphar_pg)) {
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
            logger("  Property '" + vertex_prop + "' mapping to GraphAr id saved.");
            // save map for future usage
            vertex_prop_index_map[std::make_pair(vertex->GetType(), vertex_prop)] = property_to_id_map;
        }
    }

    // 2.2 

    return "Merged successfully!";
}

