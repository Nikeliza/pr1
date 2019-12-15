#!/usr/bin/env python3
import os
import random
from cgroups import Cgroup
from pyroute2 import IPDB, NetNS, netns
import sys

btrfs_path = '/var/mocker'
cgroups1 = 'cpu,cpuacct,memory'
import btrfsutil
import subprocess
import traceback
import requests
import json
import tarfile
import uuid
import argparse


def mocker_check(uuid1):
    it = btrfsutil.SubvolumeIterator(btrfs_path, info=True, post_order=True)
    try:
        for path, info in it:
            #print(info.id, info.parent_id, path)
            if str(path) == uuid1:
                return 0
        return 1
    except Exception as e:
        print(e)
    finally:
        it.close()
        # return 1


def mocker_init(directory):
    '''
    init <directory> - создать образ контейнера
    используя указанную директорию как корневую.
    Возвращает в stdout id созданного образа.
    '''
    uuid1 = 'img_' + str(random.randint(42002, 42254))
    if os.path.exists(directory):
        if mocker_check(uuid1) == 0:
            mocker_run(directory)  # ???????????????? vse argumenti v stolbik
        btrfsutil.create_subvolume(btrfs_path + '/' + str(uuid1))
        os.system('cp -rf --reflink=auto ' + directory + '/* ' + btrfs_path + '/' + str(uuid))
        if not os.path.exists(btrfs_path + '/' + str(uuid1) + '/img.source'):
            file = open(btrfs_path + '/' + str(uuid1) + '/img.source', 'w')
            file.write(directory)
            file.close()
        print("created " + str(uuid1))
    else:
        print("Noo directory named " + directory + " exists")
    pass


def auth(library, image):
    # request a v2 token
    token_req = requests.get(
        'https://auth.docker.io/token?service=registry.docker.io&scope=repository:%s/%s:pull'
        % (library, image))
    return token_req.json()['token']


def get_manifest(image, tag, registry_base, library, headers):
    # get the image manifest
    print("Fetching manifest for %s:%s..." % (image, tag))

    manifest = requests.get('%s/%s/%s/manifests/%s' %
                            (registry_base, library, image, tag),
                            headers=headers)
    print(manifest)
    return manifest.json()


def mocker_pull(image):
    '''
    pull <image> - скачать последний (latest)
    тег указанного образа с Docker Hub.
    Возвращает в stdout id созданного образа.
    a = dockerhub.DockerHub()
    a.get_repository(image)
    '''
    registry_base = 'https://registry-1.docker.io/v2'
    library = 'library'
    # login anonymously
    headers = {'Authorization': 'Bearer %s' % auth(library,
                                                   image)}
    # get the manifest
    manifest = get_manifest(image, 'latest', registry_base, library, headers)

    # save the manifest
    image_name_friendly = manifest['name'].replace('/', '_')
    with open(os.path.join(btrfs_path,
                           image_name_friendly + '.json'), 'w') as cache:
        cache.write(json.dumps(manifest))
    # save the layers to a new folder
    dl_path = os.path.join(btrfs_path, image_name_friendly, 'layers')
    if not os.path.exists(dl_path):
        os.makedirs(dl_path)

    # fetch each unique layer
    layer_sigs = [layer['blobSum'] for layer in manifest['fsLayers']]
    unique_layer_sigs = set(layer_sigs)

    # setup a directory with the image contents
    contents_path = os.path.join(dl_path, 'contents')
    if not os.path.exists(contents_path):
        os.makedirs(contents_path)

    # download all the parts
    for sig in unique_layer_sigs:
        print('Fetching layer %s..' % sig)
        url = '%s/%s/%s/blobs/%s' % (registry_base, library,
                                     image, sig)
        local_filename = os.path.join(dl_path, sig) + '.tar'

        r = requests.get(url, stream=True, headers=headers)
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        # list some of the contents..
        with tarfile.open(local_filename, 'r') as tar:
            for member in tar.getmembers()[:10]:
                print('- ' + member.name)
            print('...')

            tar.extractall(str(contents_path))
    mocker_init(dl_path)


def mocker_rmi(uuid1):
    '''+
    rmi <image_id> - удаляет
    ранее созданный образ из локального хранилища.
    '''
    if uuid1[0: 4] == "img_":
        if mocker_check(uuid1) == 1:
            print('No image named ' + str(uuid1))
            return
        btrfsutil.delete_subvolume(btrfs_path + '/' + str(uuid1))
        cg = Cgroup(uuid1)
        cg.delete()
        print('Removed ' + str(uuid1))
    else:
        print('This is not image')


def mocker_rm(uuid1):
    '''+
    rm <container_id> - удаляет ранее
    созданный контейнер
    '''
    if uuid1[0: 3] == "ps_":
        if mocker_check(uuid1) == 1:
            print('No container named ' + str(uuid1))
            return
        btrfsutil.delete_subvolume(btrfs_path + '/' + str(uuid1))
        cg = Cgroup(uuid1)
        cg.delete()
        print('Removed ' + str(uuid1))
    else:
        print('This is not container')


def mocker_images():
    '''+
    images - выводит список локальный образов
    '''
    for image_file in os.listdir(btrfs_path):
        if image_file[0:4] == 'img_':
            file = open(btrfs_path + '/' + image_file + '/img.source', 'r')
            directory = file.read()
            file.close()
            print(image_file, directory)


def mocker_ps():
    '''+
    ps - выводит список контейнеров
    '''
    for ps_file in os.listdir(btrfs_path):
        if ps_file[0:3] == 'ps_':
            file = open(btrfs_path + '/' + ps_file + '/' + ps_file + '.cmd', 'r')
            cmd = file.read()
            file.close()
            print(ps_file, cmd)
    pass


def mocker_run(uuid1, *args):
    '''
    run <image_id> <command> - создает контейнер
    из указанного image_id и запускает его
    с указанной командой
    '''

    id = uuid.uuid4()
    print(id)
    uuid_name = 'ps_' + str(id.fields[5])[:4]
    print(str(id.fields[5])[:4], uuid_name)
    mac = str(id.fields[5])[:2]
    if mocker_check(uuid1) == 1:
        print('No image named ' + str(uuid1))
        return
    if mocker_check(uuid_name) == 0:
        print(uuid_name)
        print('UUID conflict, retrying...')
        # mocker_run(uuid1, args)
        return
    cmd = args
    ip_last_octet = 103

    with IPDB() as ipdb:
        veth0_name = 'veth0_' + str(uuid_name)
        veth1_name = 'veth1_' + str(uuid_name)
        netns_name = 'netns_' + str(uuid_name)
        bridge_if_name = 'bridge0'

        existing_interfaces = ipdb.interfaces.keys()

        # Create a new virtual interface
        with ipdb.create(kind='veth', ifname=veth0_name, peer=veth1_name) as i1:
            i1.up()
            if bridge_if_name not in existing_interfaces:
                ipdb.create(kind='bridge', ifname=bridge_if_name).commit()
            i1.set_target('master', bridge_if_name)

        # Create a network namespace
        netns.create(netns_name)

        # move the bridge interface into the new namespace
        with ipdb.interfaces[veth1_name] as veth1:
            veth1.net_ns_fd = netns_name

        # Use this network namespace as the database
        ns = IPDB(nl=NetNS(netns_name))
        with ns.interfaces.lo as lo:
            lo.up()
        with ns.interfaces[veth1_name] as veth1:
            veth1.address = "02:42:ac:11:00:{0}".format(mac)
            veth1.add_ip('10.0.0.{0}/24'.format(ip_last_octet))
            veth1.up()
        ns.routes.add({
            'dst': 'default',
            'gateway': '10.0.0.1'}).commit()

    btrfsutil.create_snapshot(btrfs_path + '/' + uuid1, btrfs_path + '/' + uuid_name)
    file = open(btrfs_path + '/' + uuid_name + '/' + uuid_name + '.cmd', 'r')
    file.write(cmd)
    file.close()
    cg = Cgroup(uuid_name)
    cg.set_cpu_limit(50)  # TODO : get these as command line options
    cg.set_memory_limit(500)

    def in_cgroup():
        try:
            pid = os.getpid()
            cg = Cgroup(uuid_name)

            netns.setns(netns_name)

            # add process to cgroup
            cg.add(pid)

        except Exception as e:
            traceback.print_exc()

    cmd = list(args)
    print(cmd)
    process = subprocess.Popen(cmd, preexec_fn=in_cgroup, shell=True)
    process.wait()
    print(process.stdout)
    NetNS(netns_name).close()
    netns.remove(netns_name)
    # ipdb.interfaces[veth0_name].remove()


def mocker_exec(uuid1, *argv):
    '''
    exec <container_id> <command> - запускает
    указанную команду внутри уже запущенного
    указанного контейнера
    '''
    pass


def mocker_logs(uuid1):
    '''+
    logs <container_id> - выводит логи
    указанного контейнера
    '''

    if mocker_check(uuid1) == 1:
        print('No container named ' + str(uuid1))
        return
    os.system('cat ' + btrfs_path + '/' + str(uuid1) + '/' + str(uuid1) + '.log')
    pass


def mocker_commit(uuid1, uuid2):
    '''+
    commit <container_id> <image_id> - создает новый
    образ, применяя изменения из образа
    container_id к образу image_id
    '''
    if mocker_check(uuid1) == 1:
        print('No container named ' + str(uuid1))
        return
    if mocker_check(uuid2) == 1:
        print('No image named ' + str(uuid2))
        return
    mocker_rmi(uuid2)
    btrfsutil.create_snapshot(btrfs_path + '/' + str(uuid1), btrfs_path + '/' + str(uuid2))
    print('Created ' + str(uuid2))


def mocker_help():
    '''+
    help - выводит help по командам
    '''
    print("формат комманды")
    print("command [argumnts]")
    print("виды команд")
    print('help/tвыводит help по командам')
    print('commit arg1 arg2/tсоздает новый образ, применяя изменения из образа arg1 к образу arg2')
    print('logs arg1/tвыводит логи указанного контейнера')
    print('exec arg1 arg2 .../tзапускает указанную команду arg2 ... внутри уже запущенного указанного контейнера arg1')
    print('run arg1 arg2 .../tсоздает контейнер из указанного image arg1 и запускает его с указанной командой arg2 ...')
    print('ps/tвыводит список контейнеров')
    print('images/tвыводит список локальный образов')
    print('rm arg1/tудаляет ранее созданный контейнер arg1')
    print('rmi arg1/tудаляет ранее созданный образ arg1 из локального хранилища.')
    print('pull arg1/tскачать последний (latest) тег указанного образа arg1 с Docker Hub.')
    print('init arg1/tсоздать образ контейнера используя указанную директорию arg1 как корневую.')
    pass


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Do not enter command')
    elif sys.argv[1] == 'help':
        mocker_help()
    elif sys.argv[1] == 'ps':
        mocker_ps()
    elif sys.argv[1] == 'images':
        mocker_images()
    elif len(sys.argv) < 3:
        print('Do not enter argument')
    elif sys.argv[1] == 'init':
        mocker_init(sys.argv[2])
    elif sys.argv[1] == 'pull':
        mocker_pull(sys.argv[2])
    elif sys.argv[1] == 'rm':
        mocker_rm(sys.argv[2])
    elif sys.argv[1] == 'rmi':
        mocker_rmi(sys.argv[2])
    elif sys.argv[1] == 'logs':
        mocker_logs(sys.argv[2])
    elif len(sys.argv) < 4:
        print('Do not enter argument')
    elif sys.argv[1] == 'commit':
        mocker_commit(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'run':
        mocker_run(sys.argv[2], sys.argv[3:])
    elif sys.argv[1] == 'exec':
        mocker_exec(sys.argv[2], sys.argv[3:])
    else:
        print('Unknown command')
