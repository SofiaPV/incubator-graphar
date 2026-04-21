/*
* This file contains data structures used to parse configs 
* and functions that do parsing.
*/

#pragma once

#include <string>
#include <vector>
#include <unordered_map>

namespace py = pybind11;

struct GraphArConfig {
  std::string path;
  std::string name;
  std::string version;

  void fill(const py::dict& config_dict);
};

struct Property {
  std::string name;
  std::string data_type;
  bool is_primary;
  bool nullable;

  void fill(const py::dict& config_dict);
};

struct PropertyGroup {
  std::string file_type;
  std::vector<Property> properties;

  void fill(const py::dict& config_dict);
};

struct Source {
  std::string file_type;
  std::vector<std::string> path;
  char delimiter;
  std::unordered_map<std::string, std::string> columns;

  void fill(const py::dict& config_dict);
};

struct Vertex {
  std::string type;
  std::vector<std::string> labels;
  int chunk_size;
  std::string validate_level;
  std::string prefix;
  std::vector<PropertyGroup> property_groups;
  std::vector<Source> sources;

  void fill(const py::dict& config_dict);
};

struct MergeVertex: public Vertex {
    std::string join_on;

    void fill(const py::dict& config_dict) {
      Vertex::fill(config_dict);
      join_on = config_dict["join_on"].cast<std::string>();
    }
};

struct AdjList {
  bool ordered;
  std::string aligned_by;
  std::string file_type;

  void fill(const py::dict& config_dict);
};

struct Edge {
  std::string edge_type;
  std::string src_type;
  std::string src_prop;
  std::string src_edge_prop;
  std::string dst_type;
  std::string dst_prop;
  std::string dst_edge_prop;
  int chunk_size;
  std::string validate_level;
  std::string prefix;
  std::vector<AdjList> adj_lists;
  std::vector<PropertyGroup> property_groups;
  std::vector<Source> sources;

  void fill(const py::dict& config_dict);
};

template <typename Data>
void fill_data(std::vector<Data>& out,
               const std::vector<py::dict>& config)
{
  for (const auto& obj_dict : config) {
    Data obj;
    obj.fill(obj_dict);
    out.emplace_back(obj);
  }
}


struct ImportSchema {
  std::vector<Vertex> vertices;
  std::vector<Edge> edges;
};

struct MergeSchema {
    std::vector<MergeVertex> vertices;
    std::vector<Edge> edges;
};

struct AbstractConfig {
  GraphArConfig graphar_config;
  bool debug_mode;

  virtual void fill(const py::dict& config_dict) = 0;
};

struct MergeConfig: public AbstractConfig {
  MergeSchema merge_schema;
  virtual void fill(const py::dict& config_dict) override {
    // graph description
    auto graphar_dict = config_dict["graphar"].cast<py::dict>();
    graphar_config.fill(graphar_dict);

    // vertices + edges 
    auto schema_dict = config_dict["merge_schema"].cast<py::dict>();
    if (schema_dict.contains("vertices")) {
      auto vertices_list = schema_dict["vertices"].cast<std::vector<py::dict>>();
      fill_data(merge_schema.vertices, vertices_list);
    }

    if (schema_dict.contains("edges")) {
      auto edges_list = schema_dict["edges"].cast<std::vector<py::dict>>();
      fill_data(merge_schema.edges, edges_list);
    }
  } 
};

struct ImportConfig: public AbstractConfig {
  ImportSchema import_schema;
  virtual void fill(const py::dict& config_dict) override {
    // graph description
    auto graphar_dict = config_dict["graphar"].cast<py::dict>();
    graphar_config.fill(graphar_dict);

    // vertices + edges 
    auto schema_dict = config_dict["import_schema"].cast<py::dict>();
    auto vertices_list = schema_dict["vertices"].cast<std::vector<py::dict>>();
    fill_data(import_schema.vertices, vertices_list);

    auto edges_list = schema_dict["edges"].cast<std::vector<py::dict>>();
    fill_data(import_schema.edges, edges_list);
  } 
};