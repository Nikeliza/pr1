#!/usr/bin/env python3
import os
import random
from cgroups import Cgroup
from pyroute2 import IPDB, NetNS, netns
import sys
import btrfsutil
import subprocess
import traceback
import requests
import json
import tarfile
import uuid


btrfs_path = '/home/vagrant/mocker'
cgroups1 = 'cpu,cpuacct,memory'


def mocker_check(uuid1):
    it = btrfsutil.SubvolumeIterator(btrfs_path, info=True, post_order=True)
    try:
        for path, info in it:
            if str(path) == uuid1:
                return 0
        return 1
    except Exception as e:
        print(e)
    finally:
        it.close()


def mocker_init(directory):
    '''
    init <directory> - создать образ контейнера
    используя указанную директорию как корневую.
    Возвращает в stdout id созданного образа.
    '''
    uuid1 = 'img_' + str(random.randint(42002, 42254))
    if os.path.exists(directory):
        if mocker_check(uuid1) == 0:
            print('UUID conflict, retrying...')
            mocker_init(directory)
            return
        btrfsutil.create_subvolume(btrfs_path + '/' + str(uuid1))
        os.system('cp -rf --reflink=auto ' + directory + '/* ' + btrfs_path + '/' + str(uuid1))
        if not os.path.exists(btrfs_path + '/' + str(uuid1) + '/img.source'):
            file = open(btrfs_path + '/' + str(uuid1) + '/img.source', 'w')
            file.write(directory)
            file.close()
        print("created " + str(uuid1))
    else:
        print("Noo directory named " + directory + " exists")


def auth(library, image):
    token_req = requests.get(
        'https://auth.docker.io/token?service=registry.docker.io&scope=repository:%s/%s:pull'
        % (library, image))
    return token_req.json()['token']


def get_manifest(image, tag, registry_base, library, headers):
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
    headers = {'Authorization': 'Bearer %s' % auth(library, image)}
    manifest = get_manifest(image, 'latest', registry_base, library, headers)

    image_name_friendly = manifest['name'].replace('/', '_')
    with open(os.path.join(btrfs_path, image_name_friendly + '.json'), 'w') as cache:
        cache.write(json.dumps(manifest))

    dl_path = os.path.join(btrfs_path, image_name_friendly, 'layers')
    if not os.path.exists(dl_path):
        os.makedirs(dl_path)

    layer_sigs = [layer['blobSum'] for layer in manifest['fsLayers']]
    unique_layer_sigs = set(layer_sigs)

    contents_path = os.path.join(dl_path, 'contents')
    if not os.path.exists(contents_path):
        os.makedirs(contents_path)

    for sig in unique_layer_sigs:
        url = '%s/%s/%s/blobs/%s' % (registry_base, library,
                                     image, sig)
        local_filename = os.path.join(dl_path, sig) + '.tar'

        r = requests.get(url, stream=True, headers=headers)
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        with tarfile.open(local_filename, 'r') as tar:
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
        netns_name = 'netns_' + str(uuid_name)
        netns.remove(netns_name)
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


def mocker_run(uuid1, *args):
    '''
    run <image_id> <command> - создает контейнер
    из указанного image_id и запускает его
    с указанной командой
    '''
    id = uuid.uuid4()
    uuid_name = 'ps_' + str(id.fields[5])[:4]
    
    mac = str(id.fields[5])[:2]
    if mocker_check(uuid1) == 1:
        print('No image named ' + str(uuid1))
        return
    if mocker_check(uuid_name) == 0:
        print(uuid_name)
        print('UUID conflict, retrying...')
        return
    cmd = args
    ip_last_octet = 103

    with IPDB() as ipdb:
        veth0_name = 'veth0_' + str(uuid_name)
        veth1_name = 'veth1_' + str(uuid_name)
        netns_name = 'netns_' + str(uuid_name)
        bridge_if_name = 'bridge0'

        existing_interfaces = ipdb.interfaces.keys()

        with ipdb.create(kind='veth', ifname=veth0_name, peer=veth1_name) as i1:
            i1.up()
            if bridge_if_name not in existing_interfaces:
                ipdb.create(kind='bridge', ifname=bridge_if_name).commit()
            i1.set_target('master', bridge_if_name)

        netns.create(netns_name)

        with ipdb.interfaces[veth1_name] as veth1:
            veth1.net_ns_fd = netns_name

        ns = IPDB(nl=NetNS(netns_name))
        with ns.interfaces.lo as lo:
            lo.up()
        with ns.interfaces[veth1_name] as veth1:
            veth1.address = "02:42:ac:11:00:{0}".format(mac)
            veth1.add_ip('10.0.0.{0}/24'.format(ip_last_octet))
            veth1.up()
        ns.routes.add({'dst': 'default', 'gateway': '10.0.0.1'}).commit()

    btrfsutil.create_snapshot(btrfs_path + '/' + uuid1, btrfs_path + '/' + uuid_name)
    file_log = open(btrfs_path + '/' + uuid_name + '/' + uuid_name + '.log', 'w')
    file = open(btrfs_path + '/' + uuid_name + '/' + uuid_name + '.cmd', 'w')
    file.write(str(cmd))
    file.close()
    cg = Cgroup(uuid_name)
    cg.set_cpu_limit(50)
    cg.set_memory_limit(500)

    def in_cgroup():
        try:
            pid = os.getpid()
            cg = Cgroup(uuid_name)

            netns.setns(netns_name)
            cg.add(pid)

        except Exception as e:
            traceback.print_exc()
            file_log.write("Failed to preexecute function")
            file_log.write(e)

    cmd = list(args)
    file_log.write('Running ' + cmd[0] + '\n')
    process = subprocess.Popen(cmd, preexec_fn=in_cgroup, shell=True)
    process.wait()
    file_log.write('Error ')
    file_log.write(str(process.stderr) + '\n')
    file_log.write('Final\n')
    NetNS(netns_name).close()
    #netns.remove(netns_name)
    file_log.write('done\n')
    print('Creating', uuid_name)


def mocker_exec(uuid1, *argv):
    '''
    exec <container_id> <command> - запускает
    указанную команду внутри уже запущенного
    указанного контейнера
    '''
    netns_name = 'netns_' + str(uuid_name)
    cmd = args
    file_log = open(btrfs_path + '/' + uuid_name + '/' + uuid_name + '.log', 'a')
    file = open(btrfs_path + '/' + uuid_name + '/' + uuid_name + '.cmd', 'a')
    file.write(str(cmd))
    file.close()
    def in_cgroup():
        try:
            pid = os.getpid()
            cg = Cgroup(uuid_name)

            netns.setns(netns_name)
            cg.add(pid)

        except Exception as e:
            traceback.print_exc()
            file_log.write("Failed to preexecute function")
            file_log.write(e)

    cmd = list(args)
    file_log.write('Running ' + cmd[0] + '\n')
    process = subprocess.Popen(cmd, preexec_fn=in_cgroup, shell=True)
    process.wait()
    file_log.write('Error ')
    file_log.write(str(process.stderr) + '\n')
    file_log.write('Final\n')
    NetNS(netns_name).close()
    #netns.remove(netns_name)
    file_log.write('done\n')
    print('Creating', uuid_name)

    


def mocker_logs(uuid1):
    '''+
    logs <container_id> - выводит логи
    указанного контейнера
    '''
    if mocker_check(uuid1) == 1:
        print('No container named ' + str(uuid1))
        return
    file = open(btrfs_path + '/' + str(uuid1) + '/' + str(uuid1) + '.log', 'r')
    print(file.read())
    file.close()


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
    print('help    выводит help по командам')
    print('commit arg1 arg2    создает новый образ, применяя изменения из образа arg1 к образу arg2')
    print('logs arg1    выводит логи указанного контейнера')
    print('exec arg1 arg2    запускает указанную команду arg2 внутри уже запущенного указанного контейнера arg1')
    print('run arg1 arg2    создает контейнер из указанного image arg1 и запускает его с указанной командой arg2')
    print('ps    выводит список контейнеров')
    print('images    выводит список локальный образов')
    print('rm arg1    удаляет ранее созданный контейнер arg1')
    print('rmi arg1    удаляет ранее созданный образ arg1 из локального хранилища.')
    print('pull arg1    скачать последний (latest) тег указанного образа arg1 с Docker Hub.')
    print('init arg1    создать образ контейнера используя указанную директорию arg1 как корневую.')


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
        mocker_run(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'exec':
        mocker_exec(sys.argv[2], sys.argv[3])
    else:
        print('Unknown command')
