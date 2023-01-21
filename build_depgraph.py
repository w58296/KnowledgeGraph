import multiprocessing
import os
from collections import defaultdict
from py2neo import Graph, Node, Subgraph, Relationship
import json
import gevent
from gevent.pool import Pool
from gevent import monkey

monkey.patch_all()

NODE_TYPE = 'MavenItem'
RELATION_TYPE = 'depend'
ROOT_PATH = 'D:/Dataset'
graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j"))


def build_sub_graph(file_path, group_id, artifact_id, version):
    print(f'build sub graph for :{group_id}-{artifact_id}-{version}')
    dependency_graph = json.loads(open(f'{file_path}/target/dependency-graph.json').read())
    artifacts = dependency_graph.get('artifacts', [])
    dependencies = dependency_graph.get('dependencies', [])
    source_node = Node(NODE_TYPE, group_id=group_id, artifact_id=artifact_id, version=version)
    if artifacts and dependencies:
        nodes_map = generate_nodes(artifacts, source_node)
        relations = generate_relations(nodes_map, dependencies)
        sub_graph = Subgraph(list(nodes_map.values()), relations)
        tx = graph.begin()
        tx.create(sub_graph)
        graph.commit(tx)


def generate_nodes(artifacts, source_node):
    nodes_map = defaultdict(lambda: Node)
    nodes_map[0] = source_node
    for artifact in artifacts:
        numericId = artifact['numericId']
        groupId = artifact['groupId']
        artifactId = artifact['artifactId']
        version = artifact['version']
        scope = artifact['scopes'][0]
        optional = artifact['optional']
        nodes_map[numericId] = \
            Node(NODE_TYPE, groupId=groupId, artifactId=artifactId, version=version, scope=scope, optional=optional)
    return nodes_map


def generate_relations(nodes_map, dependencies):
    relations = []
    for dependency in dependencies:
        numberic_from = dependency['numericFrom']
        numberic_to = dependency['numericTo']
        source = nodes_map[numberic_from]
        dest_node = nodes_map[numberic_to]
        relations.append(Relationship(source, RELATION_TYPE, dest_node))
    return relations


def main():
    tasks = []
    pool = Pool(multiprocessing.cpu_count())
    group_ids = os.listdir(ROOT_PATH)
    for group_id in group_ids:
        if not os.path.isfile(f"{ROOT_PATH}/{group_id}"):
            artifact_ids = os.listdir(f"{ROOT_PATH}/{group_id}")
            for artifact_id in artifact_ids:
                versions = os.listdir(f"{ROOT_PATH}/{group_id}/{artifact_id}")
                for version in versions:
                    file_path = f"{ROOT_PATH}/{group_id}/{artifact_id}/{version}"
                    if os.path.exists(f'{file_path}/target/dependency-graph.json'):
                        tasks.append(pool.apply_async(build_sub_graph, (file_path, group_id, artifact_id, version)))

    gevent.joinall(tasks)


if __name__ == '__main__':
    main()
