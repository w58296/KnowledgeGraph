from py2neo import Graph, Node, Subgraph, Relationship
import json

NODE_TYPE = 'MavenItem'
RELATION_TYPE = 'depend'


def build_sub_graph(exact_nodes, source_gid, source_aid, source_version):
    source_node = Node(NODE_TYPE, group_id=source_gid, artifact_id=source_aid, version=source_version)
    nodes = [source_node]
    relations = []
    for node in exact_nodes:
        gid = node['groupId']
        aid = node['artifactId']
        version = node['version']
        dest_node = Node(NODE_TYPE, group_id=gid, artifact_id=aid, version=version)
        nodes.append(dest_node)
        relations.append(Relationship(source_node, RELATION_TYPE, dest_node))
    return Subgraph(nodes, relations)


def main():
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j"))
    dependency_dict = json.loads(open('dependency-new.json', 'r').read())

    for group_id in dependency_dict.keys():
        for artifact_id in dependency_dict[group_id].keys():
            for version in dependency_dict[group_id][artifact_id].keys():
                exact_nodes = dependency_dict[group_id][artifact_id][version].get('exact', [])
                fuzzy_nodes = dependency_dict[group_id][artifact_id][version].get('fuzzy', [])
                if not fuzzy_nodes and exact_nodes:
                    sub_graph = build_sub_graph(exact_nodes, group_id, artifact_id, version)
                    tx = graph.begin()
                    tx.create(sub_graph)
                    graph.commit(tx)


if __name__ == '__main__':
    main()
