#!/usr/bin/env python3
import os
import random
from cgroups import Cgroup
from pyroute2 import IPDB, NetNS, netns

btrfs_path = '/var/mocker'
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
    # uuid="img_$(shuf -i 42002-42254 -n 1)"
    # img ???????????????????????????????++++++++++++++++++++++++++
    uuid1 = 'img_' + str(random.randint(42002, 42254))
    if os.path.exists(directory):
        if mocker_check(uuid1) == 0:
            mocker_run(directory)  # ???????????????? vse argumenti v stolbik

        btrfsutil.create_subvolume(btrfs_path + '/' + str(uuid1))

        # btrfs subvolume create "$btrfs_path/$uuid" > /dev/null
        os.system('cp -rf --reflink=auto ' + directory + '/* ' + btrfs_path + '/' + str(uuid))
        # cp -rf --reflink=auto "$1"/* "$btrfs_path/$uuid" > /dev/null

        # [[ ! -f "$btrfs_path/$uuid"/img.source ]] && echo "$1" > "$btrfs_path/$uuid"/img.source
        if not os.path.exists(btrfs_path + '/' + str(uuid1) + '/img.source'):
            os.system('echo ' + directory + ' > ' + btrfs_path + '/' + str(uuid) + '/img.source')
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
    registry_base = 'https://registry-1.docker.io/v2'  # 'https://hub.docker.com/v2/'
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
    pass


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
    '''
    	echo -e "IMAGE_ID\t\tSOURCE"
	for img in "$btrfs_path"/img_*; do
		img=$(basename "$img")
		echo -e "$img\t\t$(cat "$btrfs_path/$img/img.source")"
	done

    images = [['name', 'version', 'size', 'file']]
    for image_file in os.listdir(btrfs_path):
        if image_file.endswith('.json'):
            with open(os.path.join(btrfs_path, image_file), 'r') as json_f:
                image = json.loads(json_f.read())
            image_base = os.path.join(btrfs_path, image_file.replace('.json', ''), 'layers')
            size = sum(os.path.getsize(os.path.join(image_base, f)) for f in
                       os.listdir(image_base)
                       if os.path.isfile(os.path.join(image_base, f)))
            images.append([image['name'], image['tag'], sizeof_fmt(size), image_file])
return images
'''
    for image_file in os.listdir(btrfs_path):
        if image_file[0:4] == 'img_':
            print(image_file)


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


def mocker_run(uuid1, *args):
    '''
    run <image_id> <command> - создает контейнер
    из указанного image_id и запускает его
    с указанной командой
    '''

    '''
    uuid="ps_$(shuf -i 42002-42254 -n 1)"
	[[ "$(bocker_check "$1")" == 1 ]] && echo "No image named '$1' exists" && exit 1
	[[ "$(bocker_check "$uuid")" == 0 ]] && echo "UUID conflict, retrying..." && bocker_run "$@" && return
	cmd="${@:2}" && ip="$(echo "${uuid: -3}" | sed 's/0//g')" && mac="${uuid: -3:1}:${uuid: -2}"
    '''
    #uuid = 'ps_' + str(random.randint(42002, 42254))
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
        #mocker_run(uuid1, args)
        return
    #print(args)
    cmd = args
    #ip = uuid[-3:].replace('0', '')
    #mac = uuid[-3] + ':' + uuid[-2:]
    #print(cmd, ip, mac)
    '''
    ip link add dev veth0_"$uuid" type veth peer name veth1_"$uuid"
	ip link set dev veth0_"$uuid" up
	ip link set veth0_"$uuid" master bridge0
	ip netns add netns_"$uuid"
	ip link set veth1_"$uuid" netns netns_"$uuid"
	ip netns exec netns_"$uuid" ip link set dev lo up
	ip netns exec netns_"$uuid" ip link set veth1_"$uuid" address 02:42:ac:11:00"$mac"
	ip netns exec netns_"$uuid" ip addr add 10.0.0."$ip"/24 dev veth1_"$uuid"
	ip netns exec netns_"$uuid" ip link set dev veth1_"$uuid" up
	ip netns exec netns_"$uuid" ip route add default via 10.0.0.1
    '''
    ip_last_octet = 103
    #state = json.loads(image_details['history'][0]['v1Compatibility'])

    # Extract information about this container
    #env_vars = state['config']['Env']
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

    '''
    btrfs subvolume snapshot "$btrfs_path/$1" "$btrfs_path/$uuid" > /dev/null
	echo 'nameserver 8.8.8.8' > "$btrfs_path/$uuid"/etc/resolv.conf
	echo "$cmd" > "$btrfs_path/$uuid/$uuid.cmd"
	cgcreate -g "$cgroups:/$uuid"
	: "${BOCKER_CPU_SHARE:=512}" && cgset -r cpu.shares="$BOCKER_CPU_SHARE" "$uuid"
	: "${BOCKER_MEM_LIMIT:=512}" && cgset -r memory.limit_in_bytes="$((BOCKER_MEM_LIMIT * 1000000))" "$uuid"
	cgexec -g "$cgroups:$uuid" \
		ip netns exec netns_"$uuid" \
		unshare -fmuip --mount-proc \
		chroot "$btrfs_path/$uuid" \
		/bin/sh -c "/bin/mount -t proc proc /proc && $cmd" \
		2>&1 | tee "$btrfs_path/$uuid/$uuid.log" || true
	ip link del dev veth0_"$uuid"
	ip netns del netns_"$uuid"
    '''
    btrfsutil.create_snapshot(btrfs_path + '/' + uuid1, btrfs_path + '/' + uuid_name)
    #os.system('echo \'nameserver 8.8.8.8\' > ' + btrfs_path + '/' + uuid_name + '/etc/resolv.conf')
    #os.system('echo ' + cmd + ' > "' + btrfs_path + '/' + uuid_name + '/' + uuid_name + '.cmd"')

    cg = Cgroup(uuid_name)
    cg.set_cpu_limit(50)  # TODO : get these as command line options
    cg.set_memory_limit(500)

    def in_cgroup():
        try:
            pid = os.getpid()
            cg = Cgroup(uuid_name)
            #for env in env_vars:
                #log.info('Setting ENV %s' % env)
                #os.putenv(*env.split('=', 1))
    
            # Set network namespace
            netns.setns(netns_name)

            # add process to cgroup
            cg.add(pid)
            
            #os.chroot(layer_dir)
            #if working_dir != '':
                #log.info("Setting working directory to %s" % working_dir)
                #os.chdir(working_dir)
                
        except Exception as e:
            traceback.print_exc()
            # log.error("Failed to preexecute function")
            # log.error(e)
    cmd = list(args)
    print(cmd)
    # log.info('Running "%s"' % cmd)
    process = subprocess.Popen(cmd, preexec_fn=in_cgroup, shell=True)
    process.wait()
    print(process.stdout)
        # log.error(process.stderr)

    '''except Exception as e:
        traceback.print_exc()
        log.error(e)
    finally:'''
    # log.info('Finalizing')
    NetNS(netns_name).close()
    netns.remove(netns_name)
    #print(ipdb.interfaces)
    #ipdb.interfaces[veth0_name].remove()
    # log.info('done')


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

#mocker_pull('ubuntu')
#print(mocker_images())
mocker_run('img_42026', '../bash')
