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

#include <algorithm>
#include <any>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
//#include <boost/container/flat_map.hpp> //[My]
//#include <flat_map> //[My]
#include <set>

#include "graphar/arrow/chunk_writer.h"
#include "graphar/fwd.h"
#include "graphar/graph_info.h"
#include "graphar/types.h"

#include <iostream> //[My]
#include <string_view>  //[My]

namespace arrow {
class Array;
}

namespace graphar::builder {

/**
 * @brief Edge is designed for constructing edges builder.
 *
 */
class Edge {
 private:
  IdType src_id_, dst_id_;
  bool empty_;
  mutable bool sorted_ = true;  // since we store everything in vector, we should sort it before using binary search
  mutable std::vector<std::pair<std::string_view, std::any>> properties_;

  /**
   * @brief sortrs internal vector of properties
   */
  void sort_vector_() const
  {
    std::sort(properties_.begin(), properties_.end(), [](const auto &a, const auto& b){
      return a.first < b.first;
    });
  }

  const std::any* get_property_(const std::string& name) const
  {
    auto it = std::find_if(properties_.begin(), properties_.end(),
                       [&name](const auto& p) { return p.first == name; });
    if (it == properties_.end())
      return nullptr;
    return &(it->second);
  }

 public:
  /**
   * @brief Initialize the edge with its source and destination.
   *
   * @param src_id The id of the source vertex.
   * @param dst_id The id of the destination vertex.
   */
  explicit Edge(IdType src_id, IdType dst_id)
      : src_id_(src_id), dst_id_(dst_id), empty_(true) {}

  /**
   * @brief Check if the edge is empty.
   *
   * @return true/false.
   */
  inline bool Empty() const noexcept { return empty_; }

  /**
   * @brief Get source id of the edge.
   *S
   * @return The id of the source vertex.
   */
  inline IdType GetSource() const noexcept { return src_id_; }

  /**
   * @brief Get destination id of the edge.
   *
   * @return The id of the destination vertex.
   */
  inline IdType GetDestination() const noexcept { return dst_id_; }

  /**
   * @brief Add a property to the edge.
   *
   * @param name The name of the property.
   * @param val The value of the property.
   */
  // TODO(@acezen): Enable the property to be a vector(list).
  inline void AddProperty(std::string_view name, const std::any& val) {
    empty_ = false;
    properties_.emplace_back(name, val);
    sorted_ = false;
  }

  /**
   * @brief Reserve properties_
   * 
   * @param size The size of properties_.
   */
  void Reserve(size_t size)
  {
    properties_.reserve(size);
  }

  /**
   * @brief Get a property of the edge.
   *
   * @param property The name of the property.
   * @return The value of the property.
   */
  inline const std::any& GetProperty(const std::string& property) const {
    const std::any* ptr = get_property_(property);
    if (!ptr)
      throw std::runtime_error("Key not found");
    return *ptr;
  }

  /**
   * @brief Get all properties of the edge.
   *
   * @return The vector containing all properties of the edge.
   */
  inline const std::vector<std::pair<std::string_view, std::any>>& GetProperties()
      const {
    return properties_;
  }

  /**
   * @brief Check if the edge contains a property.
   *
   * @param property The name of the property.
   * @return true/false.
   */
  inline bool ContainProperty(const std::string& property) const {
    return get_property_(property) != nullptr;
  }

  /**
   * @brief Edge's move constructor.
   */
  Edge(Edge&& other) noexcept
    : src_id_(other.src_id_), dst_id_(other.dst_id_), empty_(other.empty_)
  {
    properties_ = std::move(other.properties_);
  }

  /**
   * @brief Move asigment operator.
   */
  Edge& operator=(Edge&& other) noexcept
  {
    properties_ = std::move(other.properties_);
    src_id_ = other.src_id_;
    dst_id_ = other.dst_id_;
    empty_ = other.empty_;
    return *this;
  }

  /**
   * @brief Copy constructor.
   */
  Edge(const Edge& other) = default;

  /**
   * @brief Copy assigment.
   */
  Edge& operator=(const Edge& other) = default;
};

/**
 * @brief The compare function for sorting edges by source id.
 *
 * @param a The first edge to compare.
 * @param b The second edge to compare.
 * @return If a is less than b: true/false.
 */
inline bool cmp_src(const Edge& a, const Edge& b) {
  if(a.GetSource() != b.GetSource())
    return a.GetSource() < b.GetSource();
  return a.GetDestination() < b.GetDestination();
}

/**
 * @brief The compare function for sorting edges by destination id.
 *
 * @param a The first edge to compare.
 * @param b The second edge to compare.
 * @return If a is less than b: true/false.
 */
inline bool cmp_dst(const Edge& a, const Edge& b) {
  if (a.GetDestination() != b.GetDestination())
    return a.GetDestination() < b.GetDestination();
  return a.GetSource() < b.GetSource();
}

/**
 * @brief EdgeBuilder is designed for building and writing a collection of
 * edges.
 *
 */
class EdgesBuilder {
 public:
  /**
   * @brief Initialize the EdgesBuilder.
   *
   * @param edge_info The edge info that describes the vertex type.
   * @param prefix The absolute prefix.
   * @param adj_list_type The adj list type of the edges.
   * @param num_vertices The total number of vertices for source or destination.
   * @param validate_level The global validate level for the writer, with no
   * validate by default. It could be ValidateLevel::no_validate,
   * ValidateLevel::weak_validate or ValidateLevel::strong_validate, but could
   * not be ValidateLevel::default_validate.
   */
  explicit EdgesBuilder(
      const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
      AdjListType adj_list_type, IdType num_vertices,
      const ValidateLevel& validate_level = ValidateLevel::no_validate)
      : edge_info_(std::move(edge_info)),
        prefix_(prefix),
        adj_list_type_(adj_list_type),
        num_vertices_(num_vertices),
        validate_level_(validate_level) {
    if (validate_level_ == ValidateLevel::default_validate) {
      throw std::runtime_error(
          "default_validate is not allowed to be set as the global validate "
          "level for EdgesBuilder");
    }
    edges_.clear();
    num_edges_ = 0;
    is_saved_ = false;
    switch (adj_list_type) {
    case AdjListType::unordered_by_source:
      vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
      break;
    case AdjListType::ordered_by_source:
      vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
      break;
    case AdjListType::unordered_by_dest:
      vertex_chunk_size_ = edge_info_->GetDstChunkSize();
      break;
    case AdjListType::ordered_by_dest:
      vertex_chunk_size_ = edge_info_->GetDstChunkSize();
      break;
    default:
      vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
    }
    Reserve((num_vertices - 1) / vertex_chunk_size_ + 1);
  }

  /**
   * @brief Set the validate level.
   *
   * @param validate_level The validate level to set.
   */
  inline void SetValidateLevel(const ValidateLevel& validate_level) {
    if (validate_level == ValidateLevel::default_validate) {
      return;
    }
    validate_level_ = validate_level;
  }

  /**
   * @brief Get the validate level.
   *
   * @return The validate level of this writer.
   */
  inline ValidateLevel GetValidateLevel() const { return validate_level_; }

  /**
   * @brief Clear the edges in this EdgesBuilder.
   */
  inline void Clear() {
    edges_.clear();
    num_edges_ = 0;
    is_saved_ = false;
  }

  /**
   * @brief Add an edge to the collection.
   *
   * The validate_level for this operation could be:
   *
   * ValidateLevel::default_validate: to use the validate_level of the builder,
   * which set through the constructor or the SetValidateLevel method;
   *
   * ValidateLevel::no_validate: without validation;
   *
   * ValidateLevel::weak_validate: to validate if the adj_list type is valid,
   * and the data in builder is not saved;
   *
   * ValidateLevel::strong_validate: besides weak_validate, also validate the
   * schema of the edge is consistent with the info defined.
   *
   * @param e The edge to add.
   * @param validate_level The validate level for this operation,
   * which is the builder's validate level by default.
   * @return Status: ok or Status::Invalid error.
   */
  Status AddEdge(const Edge& e, const ValidateLevel& validate_level =
                                    ValidateLevel::default_validate) {
    // validate
    GAR_RETURN_NOT_OK(validate(e, validate_level));
    // add an edge
    IdType vertex_chunk_index = getVertexChunkIndex(e);
    if (vertex_chunk_index >= edges_.size()) {
      edges_.resize(vertex_chunk_index + 1);
    }
    edges_[vertex_chunk_index].emplace_back(e); //std::move  ??? 
    num_edges_++;
    return Status::OK();
  }

  Status Reserve(const int64_t &reserve_count) {
    edges_.resize(reserve_count);
    return Status::OK();
  }

  Status PreReserve(const std::vector<int64_t> &reserve_count) {
    edges_.resize(reserve_count.size());
    for (int64_t i = 0; i < reserve_count.size(); i++) {
      edges_[i].reserve(reserve_count[i]);
    }
    return Status::OK();
  }

  /**
   * @brief Get the current number of edges in the collection.
   *
   * @return The current number of edges in the collection.
   */
  IdType GetNum() const { return num_edges_; }

//  IdType GetPreNum() const { return pre_num_edges_; }

  /**
   * @brief Dump the collection into files.
   *
   * @return Status: ok or error.
   */
  Status Dump();
  Status Dump(int chunk);

  /**
   * @brief Construct an EdgesBuilder from edge info.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param prefix The absolute prefix.
   * @param adj_list_type The adj list type of the edges.
   * @param num_vertices The total number of vertices for source or destination.
   * @param validate_level The global validate level for the builder, default is
   * no_validate.
   */
  static Result<std::shared_ptr<EdgesBuilder>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
      AdjListType adj_list_type, IdType num_vertices,
      const ValidateLevel& validate_level = ValidateLevel::no_validate) {
    if (!edge_info->HasAdjacentListType(adj_list_type)) {
      return Status::KeyError(
          "The adjacent list type ", AdjListTypeToString(adj_list_type),
          " doesn't exist in edge ", edge_info->GetEdgeType(), ".");
    }
    return std::make_shared<EdgesBuilder>(edge_info, prefix, adj_list_type,
                                          num_vertices, validate_level);
  }

  /**
   * @brief Construct an EdgesBuilder from graph info.
   *
   * @param graph_info The graph info that describes the graph.
   * @param src_type The type of the source vertex type.
   * @param edge_type The type of the edge type.
   * @param dst_type The type of the destination vertex type.
   * @param adj_list_type The adj list type of the edges.
   * @param num_vertices The total number of vertices for source or destination.
   * @param validate_level The global validate level for the builder, default is
   * no_validate.
   */
  static Result<std::shared_ptr<EdgesBuilder>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      const AdjListType& adj_list_type, IdType num_vertices,
      const ValidateLevel& validate_level = ValidateLevel::no_validate) {
    auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
    if (!edge_info) {
      return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                              dst_type, " doesn't exist.");
    }
    return Make(edge_info, graph_info->GetPrefix(), adj_list_type, num_vertices,
                validate_level);
  }

  /**
   * @brief add column name to the set of column names, so we can have std::string_view of it instead of copy
   * 
   * @param column_name The name of the column
   */
  void AddColumnName(const std::string& column_name)
  {
    column_names_.insert(column_name);
  }

  /**
   * @brief given a column name, return a string_view on it
   * 
   * @param column_name the name of the column
   */
  std::string_view GetColumnName(const std::string& column_name)
  {
      if (column_names_.find(column_name) == column_names_.end())  // if we do not have this name, we should add it
      {
          AddColumnName(column_name);
      }
      return std::string_view{*column_names_.find(column_name)};
  }

  const std::string& GetColumnName(std::string_view column_name) const
  {
      auto it = column_names_.find(column_name);
      if (it == column_names_.end())
        return nullptr;
      return *it;
  }

 private:
  /**
   * @brief Get the vertex chunk index of a given edge.
   *
   * @param e The edge to add.
   * @return The vertex chunk index of the edge.
   */
  IdType getVertexChunkIndex(const Edge& e) {
    switch (adj_list_type_) {
    case AdjListType::unordered_by_source:
      return e.GetSource() / vertex_chunk_size_;
    case AdjListType::ordered_by_source:
      return e.GetSource() / vertex_chunk_size_;
    case AdjListType::unordered_by_dest:
      return e.GetDestination() / vertex_chunk_size_;
    case AdjListType::ordered_by_dest:
      return e.GetDestination() / vertex_chunk_size_;
    default:
      return e.GetSource() / vertex_chunk_size_;
    }
  }

  /**
   * @brief Check if adding an edge is allowed.
   *
   * @param e The edge to add.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or status::InvalidOperation error.
   */
  Status validate(const Edge& e, ValidateLevel validate_level) const;

  /**
   * @brief Construct an array for a given property.
   *
   * @param type The type of the property.
   * @param property_name The name of the property.
   * @param array The constructed array.
   * @param edges The edges of a specific vertex chunk.
   * @return Status: ok or Status::TypeError error.
   */
  Status appendToArray(const std::shared_ptr<DataType>& type,
                       const std::string& property_name,
                       std::shared_ptr<arrow::Array>& array,  // NOLINT
                       const std::vector<Edge>& edges);

  /**
   * @brief Append the values for a property for edges in a specific vertex
   * chunk into the given array.
   *
   * @tparam type The data type.
   * @param property_name The name of the property.
   * @param array The array to append.
   * @param edges The edges of a specific vertex chunk.
   * @return Status: ok or Status::ArrowError error.
   */
  template <Type type>
  Status tryToAppend(const std::string& property_name,
                     std::shared_ptr<arrow::Array>& array,  // NOLINT
                     const std::vector<Edge>& edges);

  /**
   * @brief Append the adj list for edges in a specific vertex chunk
   * into the given array.
   *
   * @param src_or_dest Choose to append sources or destinations.
   * @param array The array to append.
   * @param edges The edges of a specific vertex chunk.
   * @return Status: ok or Status::ArrowError error.
   */
  Status tryToAppend(int src_or_dest,
                     std::shared_ptr<arrow::Array>& array,  // NOLINT
                     const std::vector<Edge>& edges);

  /**
   * @brief Convert the edges in a specific vertex chunk into
   * an Arrow Table.
   *
   * @param edges The edges of a specific vertex chunk.
   */
  Result<std::shared_ptr<arrow::Table>> convertToTable(
      const std::vector<Edge>& edges);

  /**
   * @brief Construct the offset table if the adj list type is ordered.
   *
   * @param vertex_chunk_index The corresponding vertex chunk index.
   * @param edges The edges of a specific vertex chunk.
   */
  Result<std::shared_ptr<arrow::Table>> getOffsetTable(
      IdType vertex_chunk_index, const std::vector<Edge>& edges);

 private:
  std::shared_ptr<EdgeInfo> edge_info_;
  std::string prefix_;
  AdjListType adj_list_type_;
  std::vector<std::vector<Edge>> edges_;
  IdType vertex_chunk_size_;
  IdType num_vertices_;
  IdType num_edges_;
  bool is_saved_;
  ValidateLevel validate_level_;

  struct StringComparator{
    using is_transparent = void;

    bool operator()(const std::string& a, const std::string& b) const noexcept
    {
      return a < b;
    }

    bool operator()(std::string_view a, const std::string& b) const noexcept
    {
      std::cout << "Comparing view, string\n";
      return a < b;
    }

    bool operator()(const std::string& a, std::string_view b) const noexcept
    {
      std::cout << "Comparing string, view\n";
      return a < b;
    }
  };
  std::set<std::string, StringComparator> column_names_;  // we store all possible column names here, so we don't have to store multiple copies of them
  bool InOrder = true;                  // DEBUG: for later usage of vector 

};

}  // namespace graphar::builder
