import json
import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import thread

import requests
from lxml import etree
from lxml.etree import XMLSyntaxError
import logging

ROOT_PATH = 'D:/Dataset'


# 解析策略
# 有版本解析版本, 无版本向上找dependencyManagement，遇到$尝试当前property解析，解析不到继续向上寻找

# 解析项目依赖的组件
def parse_dependency(gid, aid, version, gav_dict):
    root_path = f"{ROOT_PATH}/{gid}/{aid}/{version}"
    file_path = f"{root_path}/{aid}-{version}.pom"
    root, ns = parse_pom(file_path)
    dependency_list = root.findall('dependencies/dependency', namespaces=ns)
    further_parse_list = []
    depend_gav_list = []
    for dependency in dependency_list:
        depend_gid = dependency.find('groupId', namespaces=ns)
        depend_aid = dependency.find('artifactId', namespaces=ns)
        depend_version = dependency.find('version', namespaces=ns)
        scope = dependency.find('scope', namespaces=ns)
        if scope is None or (scope is not None and scope.text == 'compile'):
            depend_gid = parse_text_value(root, ns, depend_gid.text, gid)
            depend_aid = parse_text_value(root, ns, depend_aid.text, aid)
            if depend_version is not None:
                try:
                    depend_version = parse_text_value(root, ns, depend_version.text, version)
                    depend_gav_list.append(
                        {'groupId': depend_gid, 'artifactId': depend_aid, 'version': depend_version})
                except ValueError as e:
                    further_parse_list.append(
                        {'groupId': depend_gid, 'artifactId': depend_aid, 'version': depend_version})
            else:
                further_parse_list.append(
                    {'groupId': depend_gid, 'artifactId': depend_aid, 'version': 'empty'})
    parent_gid, parent_aid, parent_version = None, None, None
    try:
        parent_gid, parent_aid, parent_version = parser_parent_coordinate(root, ns)
    except ValueError as e:
        if further_parse_list:
            print(f"{gid}-{aid}-{version} parse error, parent is None & still need further parse")
        else:
            return depend_gav_list, further_parse_list
    parent_gid = parent_gid if parent_gid != 'empty' else gid
    parent_aid = parent_aid if parent_aid != 'empty' else aid
    parent_version = parent_version if parent_version != 'empty' else version
    further_parse(parent_gid, parent_aid, parent_version, root_path, depend_gav_list, further_parse_list)
    gav_dict[gid][aid][version] = {'exact': depend_gav_list, 'fuzzy': further_parse_list}


def further_parse(gid, aid, version, root_path, depend_gav_list, further_parse_list):
    file_path = f"{root_path}/{aid}-{version}.pom"
    root, ns = parse_pom(file_path, gid, aid, version)
    dependencies = root.findall('dependencyManagement/dependencies/dependency', namespaces=ns)
    further_dict = further_parse_dict(further_parse_list)
    for dependency in dependencies:
        depend_gid = parse_text_value(root, ns, dependency.find('groupId', namespaces=ns).text, gid)
        depend_aid = parse_text_value(root, ns, dependency.find('artifactId', namespaces=ns).text, aid)
        if further_dict[depend_gid] and further_dict[depend_gid][depend_aid]:
            if further_dict[depend_gid][depend_aid].startswith('$'):
                depend_version = parse_text_value(root, ns, further_dict[depend_gid][depend_aid], version)
            elif further_dict[depend_gid][depend_aid] == 'empty':
                depend_version = parse_text_value(root, ns, dependency.find('version', namespaces=ns).text, version)
            else:
                depend_version = ''
            further_parse_list.remove(
                {'groupId': depend_gid, 'artifactId': depend_aid, 'version': further_dict[depend_gid][depend_aid]})
            if not depend_version.startswith('$') and depend_version != 'empty':
                depend_gav_list.append({'groupId': depend_gid, 'artifactId': depend_aid, 'version': depend_version})
            else:
                further_parse_list.append({'groupId': depend_gid, 'artifactId': depend_aid, 'version': depend_version})

    if further_parse_list:
        try:
            parent_gid, parent_aid, parent_version = parser_parent_coordinate(root, ns)
            parent_gid = parent_gid if parent_gid != 'empty' else gid
            parent_aid = parent_aid if parent_aid != 'empty' else aid
            parent_version = parent_version if parent_version != 'empty' else version
            further_parse(parent_gid, parent_aid, parent_version, root_path, depend_gav_list, further_parse_list)
        except ValueError as e:
            if further_parse_list:
                print(f"{gid}-{aid}-{version} parse error, parent is None & still need further parse")


def parse_pom(file_path, gid=None, aid=None, version=None, retry=3):
    if not os.path.exists(file_path):
        i = 0
        while i < retry:
            resp = requests.get(
                f"https://repo1.maven.org/maven2/{gid.replace('.', '/')}/{aid.replace('.', '/')}/{version}/"
                f"{aid}-{version}.pom")
            try:
                resp.raise_for_status()
                with open(file_path, 'w') as f:
                    f.write(resp.text)
                    break
            except requests.HTTPError:
                i += 1
        if not os.path.exists(file_path):
            raise ValueError('download the parent pom error')
    parser = etree.XMLParser(encoding="utf-8")
    element_tree = etree.parse(rf'{file_path}', parser=parser)
    root = element_tree.getroot()
    return root, root.nsmap


def further_parse_dict(further_parse_list):
    further_dict = defaultdict(lambda: defaultdict(str))
    for further in further_parse_list:
        further_gid = further['groupId']
        further_aid = further['artifactId']
        further_dict[further_gid][further_aid] = further['version']
    return further_dict


def parse_text_value(root, ns, element, default_value):
    if element:
        if element.startswith('$'):
            name = element.split('$', 1)[-1].replace('{', '').replace('}', '')
            if not re.match(r'\$\{(project|pom)\..+\}', element):
                properties = root.find(f"properties/{name}", namespaces=ns)
                if properties is None:
                    return element
                else:
                    return parse_text_value(root, ns, properties.text, 'empty')
            else:
                properties = root.find(f"{name}", namespaces=ns)
                if properties is None:
                    return default_value
                else:
                    return parse_text_value(root, ns, properties.text, None)
        else:
            return element
    else:
        return 'empty'


def parser_parent_coordinate(root, ns):
    parent = root.find('parent', namespaces=ns)
    if parent is not None:
        # parent version must be exact
        parent_gid = parse_text_value(root, ns, parent.find('groupId', ns).text, None)
        parent_aid = parse_text_value(root, ns, parent.find('artifactId', ns).text, None)
        parent_version = parse_text_value(root, ns, parent.find('version', ns).text, None)
        return parent_gid, parent_aid, parent_version

    raise ValueError('empty parent information')


def main():
    group_ids = os.listdir(ROOT_PATH)
    gav_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    cpt = 0
    error_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    gav_dict.update(json.load(open('dependency-new.json', 'r')))
    error_dict.update(json.load(open('error-new.json', 'r')))
    for group_id in group_ids:
        if not os.path.isfile(f"{ROOT_PATH}/{group_id}"):
            artifact_ids = os.listdir(f"{ROOT_PATH}/{group_id}")
            for artifact_id in artifact_ids:
                versions = os.listdir(f"{ROOT_PATH}/{group_id}/{artifact_id}")
                for version in versions:
                    cpt += 1
                    try:
                        if (gav_dict[group_id] and gav_dict[group_id][artifact_id] and gav_dict[group_id][artifact_id][
                            version]) or (error_dict[group_id] and error_dict[group_id][artifact_id] and
                                          error_dict[group_id][artifact_id][version]):
                            print(f"skipping {group_id}-{artifact_id}-{version}")
                        else:
                            print(f'start parsing dependency of {group_id}-{artifact_id}-{version}')
                            parse_thread = threading.Thread(target=parse_dependency,
                                                            args=(group_id, artifact_id, version, gav_dict))
                            parse_thread.start()
                            parse_thread.join()
                            # parse_dependency(group_id, artifact_id, version, gav_dict)
                            print(f'end parsing dependency of {group_id}-{artifact_id}-{version}')
                    except Exception as e:
                        print(f"happen with error {e}")
                        # error_dict[group_id][artifact_id][version] = {'reason': str(e)}
                    if cpt % 500 == 0:
                        save_thread = threading.Thread(target=save_result, args=(gav_dict, error_dict))
                        save_thread.start()
                        save_thread.join()


def save_result(gav_dict, error_dict):
    gav_str = json.dumps(gav_dict, indent=4)
    error_str = json.dumps(error_dict, indent=4)
    with open('dependency-new.json', 'w') as f:
        f.write(gav_str)
    with open('error-new.json', 'w') as f:
        f.write(error_str)

    print('saving the file....')


def test():
    gid = 'com.alibaba'
    aid = 'druid'
    version = '1.0.30'
    parse_dependency(gid, aid, version)


if __name__ == '__main__':
    # test()
    main()
