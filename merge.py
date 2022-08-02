import json
import os

from collections import defaultdict

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


def merge_responses(query_dir_name: str):
    print(f"\nMerging responses for query '{query_dir_name}'...")

    # First figure out which are the 'required' query nodes
    query_dir_path = f"{CURRENT_DIR}/example_result_sets/{query_dir_name}"
    with open(f"{query_dir_path}/qg.json") as query_graph_file:
        query_graph = json.load(query_graph_file)
    required_qnode_keys = {qnode_key for qnode_key, qnode in query_graph["nodes"].items() if not qnode.get("is_set")}
    required_qnode_keys_sorted = sorted(list(required_qnode_keys))

    # For each ARA, organize their results by hash keys and merge their KG into an overarching KG
    results_by_hash_key = defaultdict(list)
    merged_kg = {"nodes": dict(), "edges": dict()}
    ara_response_file_names = [file_name for file_name in os.listdir(f"{query_dir_path}/ara_responses")
                               if file_name.endswith(".json")]
    for ara_file_name in ara_response_file_names:
        ara = ara_file_name.replace(".json", "")
        print(f"Starting to process {ara} result set...")
        with open(f"{query_dir_path}/ara_responses/{ara}.json") as response_file:
            response = json.load(response_file)
        results = response["message"]["results"]
        kg = response["message"]["knowledge_graph"]
        print(f"  {ara} response contains {len(results)} results, "
              f"{len(kg['nodes'])} KG nodes, {len(kg['edges'])} KG edges")
        # Organize results by their hash keys
        for result in results:
            qnode_keys_fulfilled_in_result = set(result["node_bindings"])
            if not required_qnode_keys.issubset(qnode_keys_fulfilled_in_result):
                print(f"  WARNING: Found a result that doesn't fulfill all required qnode keys! Skipping...")
            else:
                merge_curies = []
                for qnode_key in required_qnode_keys_sorted:
                    nodes_fulfilling_this_qnode = {binding["id"] for binding in result["node_bindings"][qnode_key]}
                    # There should only be one node fulfilling this qnode since is_set=False
                    if len(nodes_fulfilling_this_qnode) > 1:
                        print(f"  WARNING: Result has more than one node fulfilling {qnode_key}, "
                              f"which has is_set=False: {nodes_fulfilling_this_qnode}")
                        # NOTE: With TRAPI 1.3, multiple nodes WILL be able to fulfill an is_set=False node within
                        # a single result IF they all have the same parent ID mapping ('query_id'); in that case
                        # the merge curie is the 'query_id' (not implemented since TRAPI 1.3 is still in dev)
                    merge_curie = list(nodes_fulfilling_this_qnode)[0]
                    merge_curies.append(merge_curie)
                result_hash_key = "--".join(merge_curies)
                results_by_hash_key[result_hash_key].append(result)
        # Merge this ARA's answer KG into the overarching KG
        for node_key, node in kg["nodes"].items():
            # Note: Node attributes should probably be merged; not doing that here for simplicity's sake
            if node_key not in merged_kg["nodes"]:
                merged_kg["nodes"][node_key] = node
        for edge_key, edge in kg["edges"].items():
            # Note: We don't merge any edges here for simplicity's sake; other edge merging approaches could be taken
            # Also: Technically different ARAs/KPs could use the same edge keys to refer to different edges; watch out
            if edge_key not in merged_kg["edges"]:
                merged_kg["edges"][edge_key] = edge

    # Then go through and merge all results with equivalent hash keys; we want the UNION of nodes
    merged_results = []
    print(f"Merging result sets from all ARAs...")
    for hash_key, results in results_by_hash_key.items():
        merged_result = {"node_bindings": defaultdict(list), "edge_bindings": defaultdict(list)}
        for result in results:
            for qnode_key, node_bindings in result["node_bindings"].items():
                for node_binding in node_bindings:
                    merged_result["node_bindings"][qnode_key].append(node_binding)
                    # NOTE: Merging of node binding 'attributes' is not handled here..
            for qedge_key, edge_bindings in result["edge_bindings"].items():
                for edge_binding in edge_bindings:
                    # We choose to retain ALL edges (could sub in different edge merging strategy here)
                    merged_result["edge_bindings"][qedge_key].append(edge_binding)
            # NOTE: Optionally might have some way of also merging result score?
        merged_results.append(merged_result)
    print(f"Done merging results for {query_dir_name}! There are {len(merged_results)} results after merging. "
          f"Merged KG contains {len(merged_kg['nodes'])} nodes and {len(merged_kg['edges'])} edges.")

    # Get rid of duplicate node bindings
    for merged_result in merged_results:
        for qnode_key, node_bindings in merged_result["node_bindings"].items():
            deduplicated_node_bindings = []
            bound_curies = set()
            for node_binding in node_bindings:
                node_id = node_binding["id"]
                if node_id not in bound_curies:
                    deduplicated_node_bindings.append(node_binding)
                    bound_curies.add(node_id)
                # NOTE: Node bindings can have 'attributes', which perhaps should be merged here? Ignoring for now..

    # Save the merged TRAPI response
    merged_response = {"message": {"results": merged_results,
                                   "query_graph": query_graph,
                                   "knowledge_graph": merged_kg}}
    with open(f"{query_dir_path}/merged_response.json", "w+") as merged_response_file:
        json.dump(merged_response, merged_response_file, indent=2)


def main():
    example_queries = ["treats_query", "insulin-atelectasis_2hop_query"]
    for example_query in example_queries:
        merge_responses(query_dir_name=example_query)


if __name__ == "__main__":
    main()
