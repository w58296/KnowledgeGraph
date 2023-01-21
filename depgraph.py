# TODO 使用https://github.com/ferstl/depgraph-maven-plugin插件生成组件依赖
import json
import multiprocessing
import random
import shutil
import os
import subprocess
import time
# import gevent
# from gevent.pool import Pool
# from gevent import monkey
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

ROOT_PATH = 'D:/Dataset'
POOL_SIZE = 200


# monkey.patch_all()


def dependency_graph(file_path, aid, version):
    print(f'creating the {file_path}/pom.xml')
    if not os.path.exists(f'{file_path}/pom.xml'):
        shutil.copy(f"{file_path}/{aid}-{version}.pom", f'{file_path}/pom.xml')
    os.chdir(file_path)
    if not os.path.exists(f'{file_path}/target/dependency-graph.json'):
        # subprocess.Popen('mvn depgraph:graph -DgraphFormat=json -DshowGroupIds=true -DshowVersions=true',
        #                  shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        os.system('mvn depgraph:graph -DgraphFormat=json -DshowGroupIds=true -DshowVersions=true')
        time.sleep(2)


def main():
    group_ids = os.listdir(ROOT_PATH)
    pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() + 4)
    # tasks = []
    # pool = Pool(multiprocessing.cpu_count())
    count = 0
    total = 0
    visited_group_ids = []
    for group_id in group_ids:
        if group_id == 'org.webjars.npm' or group_id == 'com.github.noraui':
            continue
        visited_group_ids.append(group_id)
        if not os.path.isfile(f"{ROOT_PATH}/{group_id}"):
            artifact_ids = os.listdir(f"{ROOT_PATH}/{group_id}")
            for artifact_id in artifact_ids:
                versions = os.listdir(f"{ROOT_PATH}/{group_id}/{artifact_id}")
                for version in versions:
                    total += 1
                    # thread = Thread(target=dependency_graph,
                    #                 args=(f"{ROOT_PATH}/{group_id}/{artifact_id}/{version}", artifact_id, version))
                    # dependency_graph(f"{ROOT_PATH}/{group_id}/{artifact_id}/{version}", artifact_id, version)
                    # thread.start()
                    print(f'{group_id}-{artifact_id}-{version}')
                    file_path = f"{ROOT_PATH}/{group_id}/{artifact_id}/{version}"
                    # if not os.path.exists(f'{file_path}/target/dependency-graph.json'):
                    #     tasks.append(pool.apply_async(dependency_graph,
                    #                                   (f"{ROOT_PATH}/{group_id}/{artifact_id}/{version}", artifact_id,
                    #                                    version)))
                    if not os.path.exists(f'{file_path}/target/dependency-graph.json'):
                        # pool.submit(lambda param: dependency_graph(*param),
                        #             (f"{ROOT_PATH}/{group_id}/{artifact_id}/{version}", artifact_id,
                        #              version))
                        pass
                    else:
                        count += 1
        # if count % 8 == 0:
        #     print(f'saving the check point')
        #     time.sleep(random.randint(2, 10))
    # gevent.joinall(tasks)
    print(count)
    print(total)
    # pool.submit(wait=True)


if __name__ == '__main__':
    main()
