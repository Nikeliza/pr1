#!/usr/bin/env python3
import os
import random
from cgroups import Cgroup
from pyroute2 import IPDB, NetNS, netns

btrfs_path = '/var/mocker'
_base_dir_ = '/var/mocker'
cgroups1 = 'cpu,cpuacct,memory'
import btrfsutil
from cgroups.user import create_user_cgroups
import subprocess
import traceback
import requests
import platform
import json
import tarfile
import dockerhub
import uuid


def mocker_check(uuid1):
    it = btrfsutil.SubvolumeIterator(btrfs_path, info=True, post_order=True)
    try:
        # print(len(it))
        for path, info in it:
            print(info.id, info.parent_id, path)
            if str(path) == uuid1:
                print('ccccccccccccccccccc')
                return 0
        print('bbbbbbbbbbbbbbbbb')
        return 1
    except Exception as e:
        print(e)
    finally:
        it.close()
        print('aaaaaaaaaaaaaaaaaaaaa')
        # return 1


def mocker_init(directory):
    '''
    init <directory> - создать образ контейнера
    используя указанную директорию как корневую.
    Возвращает в stdout id созданного образа.
    '''
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
    library = 'library'
    registry_base = 'https://registry-1.docker.io/v2'
    # login anonymously
    headers = {'Authorization': 'Bearer %s' % auth(library, image)}
    # get the manifest
    tag = 'latest'
    manifest = get_manifest(image, tag, registry_base, library, headers)

    # save the manifest
    image_name_friendly = manifest['name'].replace('/', '_')
    with open(os.path.join(_base_dir_,
                           image_name_friendly + '.json'), 'w') as cache:
        cache.write(json.dumps(manifest))
    # save the layers to a new folder
    dl_path = os.path.join(_base_dir_, image_name_friendly, 'layers')
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
        url = '%s/%s/%s/blobs/%s' % (registry_base, library, image, sig)
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

            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(tar, str(contents_path))


def mocker_rmi(uuid1):
    '''+
    rmi <image_id> - удаляет
    ранее созданный образ из локального хранилища.
    '''
    '''
    	[[ "$(bocker_check "$1")" == 1 ]] && echo "No container named '$1' exists" && exit 1
	btrfs subvolume delete "$btrfs_path/$1" > /dev/null
	cgdelete -g "$cgroups:/$1" &> /dev/null || true
	echo "Removed: $1"
    '''

    if mocker_check(uuid1) == 1:
        print('No container named ' + str(uuid1))
        return
    btrfsutil.delete_subvolume(btrfs_path + '/' + str(uuid1))
    cg = Cgroup(uuid1)
    cg.remove(uuid1)
    print('Removed ' + str(uuid1))
    # Cgroup.remove(uuid)

    pass


def mocker_rm():
    '''+
    rm <container_id> - удаляет ранее
    созданный контейнер
    '''
    pass


def mocker_images():
    '''+
    images - выводит список локальный образов
    '''
    images = list_images()
    #table = AsciiTable(images)
    print(images)


def mocker_ps():
    '''+
    ps - выводит список контейнеров
    '''
    '''
    echo -e "CONTAINER_ID\t\tCOMMAND"
	for ps in "$btrfs_path"/ps_*; do
		ps=$(basename "$ps")
		echo -e "$ps\t\t$(cat "$btrfs_path/$ps/$ps.cmd")"
    '''
    pass


def list_images():
    images = [['name', 'version', 'size', 'file']]

    for image_file in os.listdir(_base_dir_):
        if image_file.endswith('.json'):
            with open(os.path.join(_base_dir_, image_file), 'r') as json_f:
                image = json.loads(json_f.read())
            image_base = os.path.join(_base_dir_, image_file.replace('.json', ''), 'layers')
            size = sum(os.path.getsize(os.path.join(image_base, f)) for f in
                        os.listdir(image_base)
                        if os.path.isfile(os.path.join(image_base, f)))
            images.append([image['name'], image['tag'], sizeof_fmt(size), image_file])
    return images


def sizeof_fmt(num, suffix='B'):
    ''' Credit : http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size '''
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def mocker_run(uuid1, *args, **kwargs):
    '''
    run <image_id> <command> - создает контейнер
    из указанного image_id и запускает его
    с указанной командой
    '''
    images = list_images()
    image_name = uuid1#kwargs['<name>']
    ip_last_octet = 103  # TODO : configurable

    match = [i[3] for i in images if i[0] == image_name][0]

    target_file = os.path.join(_base_dir_, match)
    with open(target_file) as tf:
        image_details = json.loads(tf.read())
    # setup environment details
    state = json.loads(image_details['history'][0]['v1Compatibility'])

    # Extract information about this container
    env_vars = state['config']['Env']
    start_cmd = subprocess.list2cmdline(state['config']['Cmd'])
    working_dir = state['config']['WorkingDir']

    id = uuid.uuid1()

    # unique-ish name
    name = 'c_' + str(id.fields[5])[:4]

    # unique-ish mac
    mac = str(id.fields[5])[:2]

    layer_dir = os.path.join(_base_dir_, match.replace('.json', ''), 'layers', 'contents')

    with IPDB() as ipdb:
        veth0_name = 'veth0_' + name
        veth1_name = 'veth1_' + name
        netns_name = 'netns_' + name
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

        try:
            # setup cgroup directory for this user
            user = os.getlogin()
            create_user_cgroups(user)

            # First we create the cgroup and we set it's cpu and memory limits
            cg = Cgroup(name)
            cg.set_cpu_limit(50)  # TODO : get these as command line options
            cg.set_memory_limit(500)

            # Then we a create a function to add a process in the cgroup
            def in_cgroup():
                try:
                    pid = os.getpid()
                    cg = Cgroup(name)
                    for env in env_vars:
                        #log.info('Setting ENV %s' % env)
                        os.putenv(*env.split('=', 1))

                    # Set network namespace
                    netns.setns(netns_name)

                    # add process to cgroup
                    cg.add(pid)

                    os.chroot(layer_dir)
                    if working_dir != '':
                        #log.info("Setting working directory to %s" % working_dir)
                        os.chdir(working_dir)
                except Exception as e:
                    traceback.print_exc()
                    #log.error("Failed to preexecute function")
                    #log.error(e)

            cmd = start_cmd
            print(cmd)
            #log.info('Running "%s"' % cmd)
            process = subprocess.Popen(cmd, preexec_fn=in_cgroup, shell=True)
            process.wait()
            print(process.stdout)
            #log.error(process.stderr)
        except Exception as e:
            traceback.print_exc()
            #log.error(e)
        finally:
            #log.info('Finalizing')
            NetNS(netns_name).close()
            netns.remove(netns_name)
            ipdb.interfaces[veth0_name].remove()
            #log.info('done')


def mocker_exec():
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
    '''
    [[ "$(bocker_check "$1")" == 1 ]] && echo "No container named '$1' exists" && exit 1
	cat "$btrfs_path/$1/$1.log"
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
    '''
    [[ "$(bocker_check "$1")" == 1 ]] && echo "No container named '$1' exists" && exit 1
	[[ "$(bocker_check "$2")" == 1 ]] && echo "No image named '$2' exists" && exit 1
	bocker_rm "$2" && btrfs subvolume snapshot "$btrfs_path/$1" "$btrfs_path/$2" > /dev/null
	echo "Created: $2"
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
    pass


def mocker_help():
    '''+
    help - выводит help по командам
    '''
    pass


'''
+
'''

#mocker_pull('hello-world')
#mocker_images()
mocker_run('library/hello-world')
