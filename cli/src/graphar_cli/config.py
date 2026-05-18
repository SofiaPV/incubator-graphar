# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from enum import Enum
from logging import getLogger
from pathlib import Path
from typing import Dict, List, Literal, Optional  # TODO: move to the TYPE_CHECKING block

from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from typing_extensions import Self

from ._core import (  # type: ignore  # noqa: PGH003
    check_edge,
    check_graph,
    check_vertex,
    get_edge_count,
    get_edge_types,
    get_vertex_count,
    get_vertex_types,
    show_edge,
    show_graph,
    show_vertex,
)

logger = getLogger("graphar_cli")

# TODO: move them to constants.py

DEFAULT_FILE_TYPE = "parquet"
DEFAULT_ADJ_LIST_TYPE = "ordered_by_source"
DEFAULT_REGULAR_SEPARATOR = "_"
DEFAULT_VALIDATE_LEVEL = "weak"
DEFAULT_VERSION = "gar/v1"


class FileType(str, Enum):
    parquet = "parquet"
    csv = "csv"
    orc = "orc"
    json = "json"


class GrapArBaseConfig(BaseModel):
    path: str
    name: str
    vertex_chunk_size: Optional[int] = 100
    edge_chunk_size: Optional[int] = 1024
    file_type: FileType = DEFAULT_FILE_TYPE
    adj_list_type: Literal[
        "ordered_by_source", "ordered_by_dest", "unordered_by_source", "unordered_by_dest"
    ] = DEFAULT_ADJ_LIST_TYPE
    validate_level: Literal["no", "weak", "strong"] = DEFAULT_VALIDATE_LEVEL
    version: Optional[str] = DEFAULT_VERSION


class GraphArConfig(GrapArBaseConfig):
    @field_validator("path")
    def check_path(cls, v):
        path = Path(v).resolve().absolute()
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        elif any(path.iterdir()):
            msg = f"Warning: Path {v} already exists and contains files."
            logger.warning(msg)
        return v


class GraphArMergeConfig(GrapArBaseConfig):
    tmp_path: str = ""

    @field_validator('tmp_path', mode='before')
    @classmethod
    def validate_tmp_path(cls, v):
        if v is None or v == '':
            print(f"returning ''.")
            return ''

        path = Path(v).resolve().absolute()

        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        elif any(path.iterdir()):
            msg = f"Warning: Path {path} already exists and contains files."
            logger.warning(msg)
        
        return str(path)

    @model_validator(mode='after')
    def check_path(self):
        path = Path(self.path).resolve().absolute()
        if not path.exists():
            raise ValueError(f"Path '{path}' does not exist.")
        # check this is valid graphar in folder accroding to main .yaml
        if not check_graph(f"{self.path}/{self.name}.yaml"):
            raise ValueError("Graph is not valid.")
        return self


class Property(BaseModel):
    name: str
    data_type: Literal["bool", "int32", "int64", "float", "double", "string", "date", "timestamp"]
    is_primary: bool = False
    nullable: Optional[bool] = None

    @model_validator(mode="after")
    def check_nullable(self) -> Self:
        if self.is_primary and self.nullable:
            msg = f"Primary key `{self.name}` must not be nullable."
            raise ValueError(msg)
        if self.is_primary:
            self.nullable = False
        elif self.nullable is None:
            self.nullable = True
        return self


class PropertyGroup(BaseModel):
    properties: List[Property]
    file_type: Optional[FileType] = DEFAULT_FILE_TYPE

    @field_validator("properties")
    def check_properties_length(cls, v):
        if len(v) == 0:
            msg = "properties must not be empty."
            raise ValueError(msg)
        return v


class Source(BaseModel):
    file_type: Optional[FileType] = None
    path: List[str]  # str in xml
    delimiter: str = ","
    columns: Dict[str, str]

    @field_validator("path", mode="before")
    def check_path(cls, v):
        if not isinstance(v, str):
            raise ValueError(f"path must be a string, not <{type(v)}>")

        path = Path(v)
        pattern = str(path.relative_to(path.anchor))
        files = list(Path(path.anchor).glob(pattern))  # this is where folder is unpacked

        for file in files:
            if not file.is_file():
                msg = f"'{file.resolve().absolute()}' is not a file."
                raise ValueError(msg)

        str_pathes = [str(f) for f in files]
        if not len(files):
            msg = f"files by mask '{path.resolve().absolute()}' not found."
            raise ValueError(msg)

        return str_pathes

    @field_validator("delimiter")
    def check_delimiter(cls, v):
        if len(v) != 1:
            msg = "delimiter must be a single character."
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def check_file_type(self) -> Self:
        if not self.file_type:
            file_type = Path(self.path[0]).suffix.removeprefix(".")
            if file_type == "":
                msg = f"File {self.path[0]} has no file type suffix"
                raise ValueError(msg)
            if file_type not in FileType.__members__:
                msg = f"Invalid file type '{file_type}'"
                raise ValueError(msg)
            self.file_type = file_type
        return self


class Vertex(BaseModel):
    type: str
    labels: List[str] = []
    chunk_size: Optional[int] = None
    validate_level: Optional[Literal["no", "weak", "strong"]] = None
    prefix: Optional[str] = None
    property_groups: List[PropertyGroup]
    sources: List[Source]

    @field_validator("property_groups")
    def check_property_groups_length(cls, v):
        if len(v) == 0:
            msg = "property_groups must not be empty."
            raise ValueError(msg)
        return v

    @field_validator("sources")
    def check_sources_length(cls, v):
        if len(v) == 0:
            msg = "sources must not be empty."
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def check_vertex_prefix(self) -> Self:
        prefix = self.prefix
        type = self.type
        if not prefix:
            self.prefix = f"vertex/{type}/"
        return self

class MergeVertex(Vertex):
    join_on: str

class AdjList(BaseModel):
    ordered: bool
    aligned_by: Literal["src", "dst"]
    file_type: Optional[FileType] = None


class Edge(BaseModel):
    edge_type: str
    src_type: str
    src_prop: str
    src_edge_prop: str = None # Field(default_factory=lambda validate_data: validate_data["src_prop"]) # pd2.10
    dst_type: str
    dst_prop: str
    dst_edge_prop: str = None # Field(default_factory=lambda validate_data: validate_data["dst_prop"]) # pd2.10
    chunk_size: Optional[int] = None
    validate_level: Optional[Literal["no", "weak", "strong"]] = None
    adj_lists: List[AdjList] = []
    property_groups: List[PropertyGroup] = []
    sources: List[Source]
    prefix: Optional[str] = None

    @field_validator("sources")
    def check_sources_length(cls, v):
        if len(v) == 0:
            msg = "sources must not be empty."
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def check_prefix(self) -> Self:
        prefix = self.prefix
        src_type = self.src_type
        edge_type = self.edge_type
        dst_type = self.dst_type
        if not prefix:
            self.prefix = (
                f"edge/{src_type}"
                f"{DEFAULT_REGULAR_SEPARATOR}{edge_type}"
                f"{DEFAULT_REGULAR_SEPARATOR}{dst_type}/"
            )
        return self

    @model_validator(mode="after")
    def check_edge_props(self) -> Self:
        if self.src_edge_prop is None:
            self.src_edge_prop = self.src_prop
        if self.dst_edge_prop is None:
            self.dst_edge_prop = self.dst_prop
        return self


class MergeEdge(Edge):
    # left for future modifications
    pass


class MergeSchema(BaseModel):
    vertices: List[MergeVertex] = list()
    edges: List[MergeEdge] = list()

        
class ImportSchema(BaseModel):
    vertices: List[Vertex]
    edges: List[Edge]

    @field_validator("vertices")
    def check_property_groups_length(cls, v):
        if len(v) == 0:
            msg = "vertices must not be empty."
            raise ValueError(msg)
        return v


class MergeConfig(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    graphar: GraphArMergeConfig
    merge_schema: MergeSchema
    debug_mode: bool

    @model_validator(mode="after")
    def check_merge(self) -> Self:
        for vertex in self.merge_schema.vertices:
            # TODO: get real chuck size ??? 
            if vertex.chunk_size is None:
                vertex.chunk_size = self.graphar.vertex_chunk_size
            if vertex.validate_level is None:
                vertex.validate_level = self.graphar.validate_level

            # check if 'join_on' field exists
            path_to_graph = f"{self.graphar.path}/{self.graphar.name}.yaml"
            vertex_description =  show_vertex(path_to_graph, vertex.type)
            if vertex.join_on not in vertex_description:
                msg=f"Join_on field ({vertex.join_on}) not found in schema."
                raise ValueError(msg)
        
        possible_vertex_types = get_vertex_types(f"{self.graphar.path}/{self.graphar.name}.yaml")
        for edge in self.merge_schema.edges:
            # TODO: get real params ???
            if edge.chunk_size is None:
                edge.chunk_size = self.graphar.edge_chunk_size
            if edge.validate_level is None:
                edge.validate_level = self.graphar.validate_level
            # check src_type & dst_type vertices exist
            if edge.src_type not in possible_vertex_types:
                msg = f"Edge src: {edge.src_type} does not exist."
                raise ValueError(msg)
            if edge.dst_type not in possible_vertex_types:
                msg = f"Edge dst: {edge.dst_type} does not exist."
                raise ValueError(msg)


class ImportConfig(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    graphar: GraphArConfig
    import_schema: ImportSchema
    debug_mode: bool

    @model_validator(mode="after")
    def check_none_types(self) -> Self:
        for vertex in self.import_schema.vertices:
            if vertex.chunk_size is None:
                vertex.chunk_size = self.graphar.vertex_chunk_size
            if vertex.validate_level is None:
                vertex.validate_level = self.graphar.validate_level
            for property_group in vertex.property_groups:
                if property_group.file_type is None:
                    property_group.file_type = self.graphar.file_type
        for edge in self.import_schema.edges:
            if edge.chunk_size is None:
                edge.chunk_size = self.graphar.edge_chunk_size
            if edge.validate_level is None:
                edge.validate_level = self.graphar.validate_level
            if len(edge.adj_lists) == 0:
                if self.graphar.adj_list_type == "ordered_by_source":
                    edge.adj_lists.append(
                        AdjList(ordered=True, aligned_by="src", file_type=self.graphar.file_type)
                    )
                elif self.graphar.adj_list_type == "ordered_by_dest":
                    edge.adj_lists.append(
                        AdjList(ordered=True, aligned_by="dst", file_type=self.graphar.file_type)
                    )
                elif self.graphar.adj_list_type == "unordered_by_source":
                    edge.adj_lists.append(
                        AdjList(ordered=False, aligned_by="src", file_type=self.graphar.file_type)
                    )
                elif self.graphar.adj_list_type == "unordered_by_dest":
                    edge.adj_lists.append(
                        AdjList(ordered=False, aligned_by="dst", file_type=self.graphar.file_type)
                    )
                else:
                    msg = f"Invalid adj_list_type '{self.graphar.adj_list_type}'"
                    raise ValueError(msg)
            for adj_list in edge.adj_lists:
                if adj_list.file_type is None:
                    adj_list.file_type = self.graphar.file_type
            for property_group in edge.property_groups:
                if property_group.file_type is None:
                    property_group.file_type = self.graphar.file_type

        return self
