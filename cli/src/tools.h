/*TODO: description*/

#pragma once

#include "dataclasses.h"

#include <iostream>
#include <fstream>

void logger(std::string message)
{
        std::ifstream statm("/proc/self/statm");
        size_t size, resident;
        statm >> size >> resident;
        std::cout << "[" << resident*getpagesize()/(1024) << " Kb] " << message
                  << std::endl;
}

namespace py = pybind11;
namespace fs = std::filesystem;

void GraphArConfig::fill(const py::dict& config_dict) {
  path = config_dict["path"].cast<std::string>();
  name = config_dict["name"].cast<std::string>();
  version = config_dict["version"].cast<std::string>();
}

void Property::fill(const py::dict& config_dict) {
  name = config_dict["name"].cast<std::string>();
  data_type = config_dict["data_type"].cast<std::string>();
  is_primary = config_dict["is_primary"].cast<bool>();
  nullable = config_dict["nullable"].cast<bool>();
}

void PropertyGroup::fill(const py::dict& config_dict) {
  file_type = config_dict["file_type"].cast<std::string>();
  auto prop_list = config_dict["properties"].cast<std::vector<py::dict>>();
  for (const auto& prop_dict : prop_list) {
    Property prop;
    prop.fill(prop_dict);
    properties.emplace_back(prop);
  }
}

void Source::fill(const py::dict& config_dict) {
  file_type = config_dict["file_type"].cast<std::string>();
  path = config_dict["path"].cast<std::vector<std::string>>();
  delimiter = config_dict["delimiter"].cast<char>();
  columns = config_dict["columns"].cast<std::unordered_map<std::string, std::string>>();
}

void Vertex::fill(const py::dict& config_dict) {
  type = config_dict["type"].cast<std::string>();
  chunk_size = config_dict["chunk_size"].cast<int>();
  prefix = config_dict["prefix"].cast<std::string>();
  validate_level = config_dict["validate_level"].cast<std::string>();
  labels = config_dict["labels"].cast<std::vector<std::string>>();

  auto pg_list = config_dict["property_groups"].cast<std::vector<py::dict>>();
  for (const auto& pg_dict : pg_list) {
    PropertyGroup pg;
    pg.fill(pg_dict);
    property_groups.emplace_back(pg);
  }

  auto source_list = config_dict["sources"].cast<std::vector<py::dict>>();
  for (const auto& source_dict : source_list) {
    Source src;
    src.fill(source_dict);
    sources.emplace_back(src);
  }
}

void Edge::fill(const py::dict& config_dict) {
  edge_type = config_dict["edge_type"].cast<std::string>();
  src_type = config_dict["src_type"].cast<std::string>();
  src_prop = config_dict["src_prop"].cast<std::string>();
  src_edge_prop = config_dict["src_edge_prop"].cast<std::string>();
  dst_type = config_dict["dst_type"].cast<std::string>();
  dst_prop = config_dict["dst_prop"].cast<std::string>();
  dst_edge_prop = config_dict["dst_edge_prop"].cast<std::string>();
  chunk_size = config_dict["chunk_size"].cast<int>();
  validate_level = config_dict["validate_level"].cast<std::string>();
  prefix = config_dict["prefix"].cast<std::string>();

  auto adj_list_dicts = config_dict["adj_lists"].cast<std::vector<py::dict>>();
  for (const auto& adj_list_dict : adj_list_dicts) {
    AdjList adj_list;
    adj_list.fill(adj_list_dict);
    adj_lists.emplace_back(adj_list);
  }

  auto pg_list = config_dict["property_groups"].cast<std::vector<py::dict>>();
  for (const auto& pg_dict : pg_list) {
    PropertyGroup pg;
    pg.fill(pg_dict);
    property_groups.emplace_back(pg);
  }

  auto source_list = config_dict["sources"].cast<std::vector<py::dict>>();
  for (const auto& source_dict : source_list) {
    Source src;
    src.fill(source_dict);
    sources.emplace_back(src);
  }
}

void AdjList::fill(const py::dict& config_dict) {
  ordered = config_dict["ordered"].cast<bool>();
  aligned_by = config_dict["aligned_by"].cast<std::string>();
  file_type = config_dict["file_type"].cast<std::string>();
}

/*void ConvertPyDictToConfig(const py::dict& config_dict, AbstractConfig& config) {
  // graph description
  auto graphar_dict = config_dict["graphar"].cast<py::dict>();
  config.graphar_config.fill(graphar_dict);

  // vertices + edges 
  auto schema_dict = config_dict[config.config_name()].cast<py::dict>();
  auto vertices_list = schema_dict["vertices"].cast<std::vector<py::dict>>();
  for (const auto& vertex_dict : vertices_list) {
    Vertex vertex;
    vertex.fill(vertex_dict);
    config.import_schema.vertices.emplace_back(vertex);
  }

  auto edges_list = schema_dict["edges"].cast<std::vector<py::dict>>();
  for (const auto& edge_dict : edges_list) {
    Edge edge;
    edge.fill(edge_dict);
    config.import_schema.edges.emplace_back(edge);
  }
}*/