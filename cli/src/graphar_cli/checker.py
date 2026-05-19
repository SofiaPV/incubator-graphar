from typing import Any
from pathlib import Path
import csv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import yaml
import os
import copy

import re
from typing import Dict, List, Any
import pyarrow as pa




def get_all_files(path: str) -> list[str]:
    if not os.path.exists(path):
        raise ValueError(f"Path does not exist: {path}")

    if os.path.isfile(path):
        return [path]

    result = []
    for root, _, files in os.walk(path):
        for file in files:
            result.append(os.path.join(root, file))

    return result


def compare_stats(
    reference: dict[str, dict[str, dict[str, int]]],
    candidate: dict[str, dict[str, dict[str, int]]],
) -> bool:
    success = True

    for object_type, ref_params in reference.items():
        if object_type not in candidate:
            print(f"[ERROR] Missing object type in GraphAr: '{object_type}'")
            success = False
            continue

        graphar_stats = candidate[object_type]

        for column_name, ref_stats in ref_params.items():
            if column_name not in graphar_stats:
                print(f"[ERROR] Missing column_name '{column_name}' in '{object_type}'")
                success = False
                continue

            cand_stats = graphar_stats[column_name]

            for field in ("null_count", "row_count"):
                ref_value = ref_stats.get(field)
                cand_value = cand_stats.get(field)

                if ref_value != cand_value:  # TODO: collect statistics, not actual values
                    print(
                        f"[ERROR] Mismatch in '{object_type}' -> '{column_name}' -> '{field}': "
                        f"expected={ref_value}, got={cand_value}"
                    )
                    success = False

    print(f"[INFO] No{' other ' if not success else ' '}differences found.\n")

    return success


def get_parquet_stats(
    path: str,
    columns_map: dict[str, str],
    stats: dict[str, dict[str, int]],
) -> None:
    file = pq.ParquetFile(path)

    col_indices = {
        file_col: file.schema_arrow.get_field_index(file_col)
        for file_col in columns_map
    }

    missing = [col for col, idx in col_indices.items() if idx == -1]
    if missing:
        raise ValueError(f"Columns not found in parquet: {missing}")

    for rg in range(file.num_row_groups):
        rg_meta = file.metadata.row_group(rg)
        num_rows = rg_meta.num_rows

        for file_col, out_col in columns_map.items():
            idx = col_indices[file_col]
            col_chunk = rg_meta.column(idx)

            null_count = 0
            if col_chunk.statistics is not None:
                null_count = col_chunk.statistics.null_count

            if out_col not in stats:
                stats[out_col] = {
                    "null_count": 0,
                    "row_count": 0,
                }

            stats[out_col]["null_count"] += null_count
            stats[out_col]["row_count"] += num_rows


def get_csv_stats(
    path: str,
    columns_map: dict[str, str],
    stats: dict[str, dict[str, int]],
) -> None:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        missing = set(columns_map.keys()) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns in CSV: {missing}")

        for row in reader:
            for file_col, out_col in columns_map.items():
                value = row[file_col]

                is_null = value == "" or value is None
                if out_col not in stats:
                    stats[out_col] = {
                        "null_count": 0,
                        "row_count": 0,
                    }

                if is_null:
                    stats[out_col]["null_count"] += 1

                stats[out_col]["row_count"] += 1


def detect_format(path: str) -> str:
    with open(path, "rb") as f:
        header = f.read(4)

    if header == b"PAR1":
        return "parquet"

    return "csv"


def get_stats(path: str,
    columns_map: dict[str, str],
    stats: dict[str, dict[str, int]]) -> None:
    
    fmt = detect_format(path)
    if fmt == "parquet":
        return get_parquet_stats(path, columns_map, stats)
    elif fmt == "csv":
        return get_csv_stats(path, columns_map, stats)
    else:
        raise ValueError(f"Unknown format for file: {path}")


def get_user_data_stats(obj_info, vertex: bool, exclude: list | None = None) -> dict:
    stats_correct = dict()
    if vertex:
        type_name = "type"
    else:
        type_name = "edge_type"
    for obj_info in obj_info:
        stats_correct[obj_info[type_name]] = dict()
        for source in obj_info["sources"]:
            columns = source["columns"]
            columns_cut = dict()
            for key in columns.keys():
                if not exclude or columns[key] not in exclude:
                    columns_cut[columns[key]] = columns[key]
            for file in source["path"]:
                get_stats(file, columns_cut, stats_correct[obj_info[type_name]])
    return stats_correct


def light_checker(config: dict[str, Any]) -> str:
    """Perform light check.
    
    Check includes: 
    - Comparing number of vertices & edges
    - Presense of necessory attributes
    - Column types matching
    - Comparing number of nulls in corresponding columns
    """

    #print(config["import_schema"]["edges"])
    graph_path = config["graphar"]["path"]

    # ==================== vertices ====================
    vertices = config["import_schema"]["vertices"]

    # collcect vertices stats from user data
    stats_correct = get_user_data_stats(vertices, vertex=True)
            

    # collcect vertex stats from graphar data
    stats_graphar = dict()
    for vertex_info in vertices:
        stats_graphar[vertex_info["type"]] = dict()

        path_to_vertex = graph_path + "/" + vertex_info["type"] + ".vertex.yaml"
        with open(path_to_vertex, "r", encoding="utf-8") as f:
            info = yaml.safe_load(f)

        path_to_vertex = graph_path + "/" + info["prefix"]
        for pg in info["property_groups"]:
            path_to_vetrex_pg = Path(path_to_vertex + pg["prefix"])
            columns = {prop["name"]: prop["name"] for prop in pg["properties"]}

            all_files = get_all_files(path_to_vetrex_pg)
            for file in all_files:
                get_stats(file, columns, stats_graphar[vertex_info["type"]])

    # compare results
    print(f"[INFO] Comparing vertices.")
    compare_stats(stats_correct, stats_graphar)

    # ==================== edges ====================
    edges = config["import_schema"]["edges"]

    # collect edges stats from user data
    exclude = []
    for e in edges:
        exclude.append(e["src_edge_prop"])
        exclude.append(e["dst_edge_prop"])
    stats_correct = get_user_data_stats(edges, vertex=False, exclude=exclude)

    # collect stats from GraphAr
    stats_graphar = {"ordered_by_source": dict(), "ordered_by_dest": dict()}
    path_to_description = graph_path + "/" + config["graphar"]["name"] + ".yaml"
    with open(path_to_description, "r", encoding="utf-8") as f:
        info = yaml.safe_load(f)
    for edge in info["edges"]:
        path_to_edge_description = graph_path + "/" + edge
        with open(path_to_edge_description, "r", encoding="utf-8") as f:
            edge_info = yaml.safe_load(f)

        for adj_list in edge_info["adj_lists"]:
            edge_name = edge_info['prefix'].split('_')[1]
            stats_graphar[adj_list["prefix"][:-1]][edge_name] = dict()
            if "property_groups" not in edge_info:
                # edge has no properties -> we have only topology
                # topology cannot be NULL in GraphAr (invariant),
                # and it is useless to make it NULL in our data,
                # so we skip this step.
                pass
                #print(f'{graph_path + "/" + edge_info["prefix"] + adj_list["prefix"] + "adj_list/"}')
            else:
                for pg in edge_info["property_groups"]:
                    path_to_edge = graph_path + "/" + edge_info["prefix"] + adj_list["prefix"] + pg["prefix"]

                    columns = {prop["name"]: prop["name"] for prop in pg["properties"]}
                    edge_files = get_all_files(path_to_edge)
                    for file in edge_files:
                        get_stats(file, columns, stats_graphar[adj_list["prefix"][:-1]][edge_name])
    
    # compare results
    for adj_list in stats_graphar:
        print(f"[INFO] Comparing edges {adj_list}.")
        compare_stats(stats_correct, stats_graphar[adj_list])

    return "Light check completed!"


# ======================================== in-depth checker functoins ========================================

def get_idx2pk_parquet(
    path: str,
    columns: list[str, str],
    pk2id: dict[int, int],
) -> None:
    file = pq.ParquetFile(path)

    for rg in range(file.num_row_groups):
        pk_col, graphar_id_col = columns
        table = file.read_row_group(
            rg,
            columns=[pk_col, graphar_id_col]
        )

        pk_array = table[pk_col]
        id_array = table[graphar_id_col]

        for pk, idx in zip(pk_array.to_pylist(), id_array.to_pylist()):
            if pk is not None:
                pk2id[idx] = pk



def get_PK2row(tables: list, column_name: str, pk2row: dict[int, int]) -> None:

    for table_num, table in enumerate(tables):
        if column_name not in table.schema.names:
            continue

        column = table[column_name]
        column = column.combine_chunks()
        for row_num, value in enumerate(column):
            py_value = value.as_py()

            pk2row[py_value] = {
                "table_num": table_num,
                "row_num": row_num
            }


# TODO: check this
def validate_rows(
    file_path: str,  # path to GraphAr property group file
    id2PK: Dict[int, Any],
    tables: List[pa.Table],
    id2row: Dict[Any, Dict[str, int]],
) -> List[Dict[str, Any]]:
    """
    Проверяет совпадение строк между parquet-файлом и уже загруженными таблицами.

    Возвращает список расхождений.
    """

    # --- 1. get number from file name ---
    match = re.search(r"(\d+)(?!.*\d)", file_path)
    if not match:
        raise ValueError(f"Could not extract chunk number from file: {file_path}")

    file_num = int(match.group(1))

    # --- 2. read parquet ---
    dataset = ds.dataset(file_path, format="parquet")

    statistics = dict()
    statistics["matching-rows-count"] = 0

    for batch in dataset.to_batches():
        batch = batch.to_pylist()

        for row in batch:

            # --- 3. get _grapArVertexIndex -> PK ---
            graphAr_index = row["_graphArVertexIndex"]
            pk = id2PK.get(graphAr_index)
            if pk is None:
                # this cannot happen, but just to be sure
                print(f"[ERROR] FATAL: no primary attribute for vertex with index: {graphAr_index}.")
                raise RuntimeError("Either this code or your GraphAr graph (less likely) is fundamentally broken.")

            # --- 4. look for this PK in user table ---
            mapping = id2row.get(pk)
            if mapping is None:
                if "no-value-count" not in statistics:
                    statistics["no-value-example"] = pk
                statistics["no-value-count"] = statistics.get("no-value-count", 0) + 1
                continue

            table_num = mapping["table_num"]
            table_row_num = mapping["row_num"]

            table = tables[table_num]

            # --- 5. get value from table ---
            table_data = table.slice(table_row_num, 1).to_pylist()[0]

            # --- 6. compare ---
            for column in row:  # for each value in GrphAr
                row_is_ok = True
                if column != "_graphArVertexIndex" and row[column] != table_data[column]:  # TODO: check names match 
                    if "value-mismatch-count" not in statistics:
                        statistics["value-mismatch-example"] = {
                            "value": row[column],
                            "true_value": table_data[column],
                            "column": column,
                        }
                    statistics["value-mismatch-count"] = statistics.get("value-mismatch-count", 0) + 1
                    row_is_ok = False
            if row_is_ok:
                statistics["matching-rows-count"] += 1

    return statistics


def get_pair2row(tables: list, src_name: str, dst_name: str, pair2row: dict, reverse: bool = False) -> None:

    for table_num, table in enumerate(tables):
        if src_name not in table.schema.names or dst_name not in table.schema.names:
            print(f"[WARNING] A user table does not have either '{src_name}' or '{dst_name}' column.")
            continue

        src_column = table[src_name]
        dst_column = table[dst_name]

        src_column = src_column.combine_chunks()
        dst_column = dst_column.combine_chunks()

        for row_num, (src_val, dst_val) in enumerate(zip(src_column, dst_column)):
            py_src = src_val.as_py()
            py_dst = dst_val.as_py()
            
            if py_src is None or py_dst is None:
                continue

            pair_key = (py_src, py_dst)

            if reverse:
                pair2row[(table_num, row_num)] = {
                    "src": py_src,
                    "dst": py_dst
                }
            else:
                pair2row[pair_key] = {
                    "table_num": table_num,
                    "row_num": row_num
                }



def validate_edge_chunk(tables, user_tables, idx2pk, edge2row, pair2row, src_vertex, dst_vertex, num_of_examples = 5):
    """
    tables: list of partN chunks of ONE property group, which must be checked
    user_tables: list of all user tables from one source
    idx2pk: vertex graphArIndex->PK by vertex name
    edge2row: <table_num, row_num> -> <graphArSrc, graphArDst>, gathered from adj_lists of this chunks
    pair2row: <table_num, row_num> -> <userPKsrc, userPKdst> for user tables
    """

    mismatches = {
        "no_ids": {"count": 0, "examples": []},
        "no_columns": {"count": 0, "examples": []},
        "value_mismatch": {"count": 0, "examples": []}
    }
    
    for table_num, table in enumerate(tables):
        num_rows = table.num_rows
        for row in range(num_rows):
            row_dict = {col: table.column(col)[row].as_py() for col in table.column_names}

            # find graphAr src->dst ids for this edge
            #print(f"===============\n{row_dict=}")
            #print(f"{table_num=}/{len(tables)}, {row=}/{num_rows}")
            #print(f"{edge2row=}")
            vertices = edge2row[(table_num, row)]
            #print(f"{vertices=}")
            src, dst = vertices["src"], vertices["dst"]

            # tranfer graphar src&dst to user PK
            src_pk, dst_pk = idx2pk[src_vertex][src], idx2pk[dst_vertex][dst]
            #print(f"{src_pk=}, {dst_pk=}")

            # find row in user table which contains this PKs
            if (src_pk, dst_pk) in pair2row:
                #print("found pair")
                table_row = pair2row[(src_pk, dst_pk)]
                #print(f"{table_row=}")
                data_from_user_table = user_tables[table_row["table_num"]].slice(table_row["row_num"], 1)
                #print(f"\n{data_from_user_table=}\n")

                # collect data from user table
                data_from_user_table = {col: data_from_user_table.column(col)[0].as_py() for col in data_from_user_table.column_names}
                #print(data_from_user_table)

                # compare it with GraphAr data
                for k, v in row_dict.items():
                    if k not in data_from_user_table:
                        if k == "_graphArSrcIndex" or k == "_graphArDstIndex":
                            continue

                        if k not in mismatches["no_columns"]["examples"]:
                            mismatches["no_columns"]["examples"].append(k)
                            mismatches["no_columns"]["count"] += 1
                        continue 
                    if data_from_user_table[k] != v:  # TODO table names may mismatch, rename them during reading
                        mismatches["value_mismatch"]["count"] += 1
                        if mismatches["value_mismatch"]["count"] <= num_of_examples:
                            mismatches["value_mismatch"]["examples"].append({
                                "column": k,
                                "value_true": data_from_user_table[k],
                                "value_graphar": v
                            })

            else:
                mismatches["no_ids"]["count"] += 1
                if mismatches["no_ids"]["count"] <= num_of_examples:
                    mismatches["no_ids"]["examples"].append((src_pk, dst_pk))
    return mismatches


def print_edge_statistics(stats: dict, path_to_pg) -> None:
    has_problems = False
    if stats["no_ids"]["count"] > 0:
        has_problems = True
        print(f"[INFO] [{path_to_pg}] {stats['no_ids']['count']} graphAr edges are not presented in user data:\n")
        for i, el in enumerate(stats["no_ids"]["examples"]):
            print(f"        {i}) {el[0]} -> {el[1]}")
        if stats["no_ids"]["count"] > len(stats["no_ids"]["examples"]):
            print(f"        ...")

    if stats["no_columns"]["count"] > 0:
        has_problems = True
        print(f"[INFO] [{path_to_pg}] {stats['no_columns']['count']} GraphAr columns are not presented in user data:")
        for i, el in enumerate(stats["no_columns"]["examples"]):
            print(f"        {i}) {el}")
    
    if stats["value_mismatch"]["count"] > 0:
        has_problems = True
        print(f"[INFO] [{path_to_pg}] {stats['value_mismatch']['count']} GraphAr values do not match user data:")
        for i, el in enumerate(stats["value_mismatch"]["examples"]):
            print(f"        {i}) In graphAr column {el['column']} there is value {el['value_graphar']}, but {el['value_true']} is in data.")
        if stats["value_mismatch"]["count"] > len(stats["value_mismatch"]["examples"]):
            print(f"        ...")

    if not has_problems:
        print(f"[INFO] No problems found for PG: {path_to_pg}.")


def print_statistics(stats: dict) -> None:
    if "no-value-count" in stats:
        print(f"[INFO] Found no primary attribute value in user table ({stats['no-value-count']} times).")
        print(f"       Example: found no {stats['no-value-example']} in user table.")

    if "value-mismatch-count" in stats:
        print(f"[INFO] Found mismatches in corresponding attribute values ({stats['value-mismatch-count']} times).")
        print(f"       Example: in column \'{stats['value-mismatch-example']['column']}\' graphAr has value \'{stats['value-mismatch-example']['value']}\', not {stats['value-mismatch-example']['true_value']}.")


def deep_checker(config: dict[str, Any]) -> str:
    """Perform in-depth check by comparing actual data from GraphAr with one given by user.
    
    Important-to-remember facts:
    1. Vertices in GraphAr have unique PK properties.
       That is why we can legally pick any GraphAr vertex and look for it in user data, without
       worrying about possible duplicates we might miss. This invariant is guaranteed by converter's
       ('graphar import') code, which uses std::unordered_map to identify vertex by PK property 
       before it is given a GraphAr id.
    """
    print("Starting in-depth check.")

    # ==================== graph info ====================
    graph_path = config["graphar"]["path"]

    # ==================== vertices ====================
    vertices = config["import_schema"]["vertices"]
    idx2pk = dict()
    #print(vertices)

    # read correct user data
    for vertex_info in vertices:

        vertex_tables = []
        vertex_primary_property = None
        
        # read tables
        for src in vertex_info["sources"]:
            dataset = ds.dataset(src["path"], format=src["file_type"])
            vertex_tables.append(dataset.to_table())
    
        # save primary property for this vertex
        for pg in vertex_info["property_groups"]:
            for prop in pg["properties"]:
                if prop["is_primary"]:
                    vertex_primary_property = prop["name"]
            if not vertex_primary_property:
                print(f"[ERROR] No primary property found for vertex {vertex_info['type']}, skipping it.")
                continue

        # save dict[primary_property] = line_number
        user_pk2row = dict()
        get_PK2row(vertex_tables, vertex_primary_property, user_pk2row)

        # read info about this vertex in GraphAr
        path_to_vertex_info = graph_path + "/" + vertex_info["type"] + ".vertex.yaml"
        with open(path_to_vertex_info, "r", encoding="utf-8") as f:
            info = yaml.safe_load(f)
        path_to_vertex = graph_path + "/" + info["prefix"]

        # find path to this vertex primary attribute
        path_to_pk_prop = None
        primpary_property_name = None
        for pg in info["property_groups"]:
            for prop in pg["properties"]:
                if prop["is_primary"]:
                    path_to_pk_prop = path_to_vertex + pg["prefix"]
                    primpary_property_name = prop["name"]
                    break
        if not path_to_pk_prop:
            print(f"GraphAr vertex {vertex_info['type']} has no primary column, skipping it.")
            continue

        # collect dict[graphar_index] = PK
        idx2pk[vertex_info["type"]] = dict()
        all_files = get_all_files(path_to_pk_prop)
        for file in all_files:
            get_idx2pk_parquet(file, [primpary_property_name, "_graphArVertexIndex"], idx2pk[vertex_info["type"]])

        # read data from pg
        matching_rows_count = 0
        for pg in info["property_groups"]:
            path_to_vetrex_pg = Path(path_to_vertex + pg["prefix"])
            all_files = get_all_files(path_to_vetrex_pg)
            for file in all_files:
                statistics = validate_rows(file, idx2pk[vertex_info["type"]], vertex_tables, user_pk2row)
                print_statistics(statistics)
                matching_rows_count += statistics["matching-rows-count"]
            print(f"[INFO] Values match in {matching_rows_count} rows in PG \'{pg['prefix']}\' of vertex \'{vertex_info['type']}\'.")

        # TODO clean data
        del user_pk2row
        del vertex_tables

    print()

    # ==================== edges ====================
    edges = config["import_schema"]["edges"]

    for edge_info in edges:
        edge_tables = []

        # read tables
        for src in edge_info["sources"]:  # TODO: several sources ? 
            dataset = ds.dataset(src["path"], format=src["file_type"])
            edge_tables.append(dataset.to_table())

        # select src & dst PK columns
        src_column = edge_info["src_edge_prop"]
        dst_column = edge_info["dst_edge_prop"]

        # save <src, dst> -> [table_num, row]
        pairs2row = dict()
        get_pair2row(edge_tables, src_column, dst_column, pairs2row)

        user_edge_properties = []
        for source in edge_info["sources"]:
            for col_in_file in source["columns"]:

                # append name of the column in GraphAr
                prop = source["columns"][col_in_file]
                if prop != src_column and prop != dst_column:
                    user_edge_properties.append(source["columns"][col_in_file])

        # check PGs have all user attributes
        missing_properties = []

        path_to_description = graph_path + "/" + config["graphar"]["name"] + ".yaml"
        with open(path_to_description, "r", encoding="utf-8") as f:
            info = yaml.safe_load(f)

        this_edge_exists = False
        for edge in info["edges"]:
            # read only this edge's properties
            if edge_info["edge_type"] not in edge:
                continue

            this_edge_exists = True
            path_to_edge_description = graph_path + "/" + edge
            with open(path_to_edge_description, "r", encoding="utf-8") as f:
                edge_info_graphar = yaml.safe_load(f)

            if "property_groups" not in edge_info_graphar:
                continue 
            for pg in edge_info_graphar["property_groups"]:
                for prop in pg["properties"]:
                    if prop["name"] not in user_edge_properties:
                        missing_properties.append(prop["name"])
                        break 
            
        if missing_properties:
            print(f"[WARNING] Edge \'{edge_info['edge_type']}\' has properties: {missing_properties}. No source contains them, so they cannot be checked.")
        if not this_edge_exists:
            print(f"[WARNING] Edge from config \'{edge_info['edge_type']}\' does not exist in graph.")
            continue

        # work with each adj_list in GrapgAr
        edge_full_name = edge_info_graphar["prefix"].split("/")[1]
        edge_src_vertex = edge_full_name.split('_')[0]
        edge_dst_vertex = edge_full_name.split('_')[2]

        for adj_list in edge_info_graphar["adj_lists"]:
            path_to_adj_list = graph_path + "/edge/" + edge_full_name + "/ordered_by_"+("source" if adj_list["aligned_by"] == "src" else "dest") + "/adj_list/"
            num_of_edge_chunks = len([d for d in os.listdir(path_to_adj_list) if os.path.isdir(os.path.join(path_to_adj_list, d)) and not d.startswith('.')])
            
            # get an edge chunk
            for edge_chunk in range(num_of_edge_chunks):
                # files should be in correct order 0->N
                all_files = sorted(get_all_files(path_to_adj_list+f"part{edge_chunk}"))
                #print(all_files)

                adj_lists_tables = []
                for src in all_files:
                    dataset = ds.dataset(src, format='parquet')  # TODO: GraphAr can store data in any type, not just parquet
                    adj_lists_tables.append(dataset.to_table())
                edge2row = dict()
                get_pair2row(adj_lists_tables, "_graphArSrcIndex", "_graphArDstIndex", edge2row, reverse=True)


                # edge might have no PG
                if "property_groups" not in edge_info_graphar:
                    mismatches = validate_edge_chunk(adj_lists_tables, edge_tables, idx2pk, edge2row, pairs2row, edge_src_vertex, edge_dst_vertex)
                    print_edge_statistics(mismatches, edge_full_name)
                    continue

                # we need this only to know the positions of edges for later usgae
                del adj_lists_tables

                # now we need to read one corresponding PG chunk at a time
                for pg in edge_info_graphar["property_groups"]:
                    path_to_pg = graph_path + "/edge/" + edge_full_name + "/ordered_by_"+("source/" if adj_list["aligned_by"] == "src" else "dest/") +\
                                 pg["prefix"] + f"part{edge_chunk}/"

                    all_chunk_names = sorted(get_all_files(path_to_pg))
                    chunk_files = []
                    for chunk_file in all_chunk_names:
                        dataset = ds.dataset(chunk_file, format='parquet')
                        chunk_files.append(dataset.to_table())
                    
                    mismatches = validate_edge_chunk(chunk_files, edge_tables, idx2pk, edge2row, pairs2row, edge_src_vertex, edge_dst_vertex)
                    print_edge_statistics(mismatches, path_to_pg)            

    return "In-depth check completed!"


def check_graphar(config: dict[str, Any], light_check: bool, deep_check: bool) -> str:
    """Perform both light and/or in-depth GraphAr result check according to correct data provided by user."""
    if light_check:
        res = light_checker(config)
        print(f"{res}\n")

    if deep_check:
        res = deep_checker(config)
        print(res)

    return "Comparison complete."
