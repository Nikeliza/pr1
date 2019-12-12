#!/usr/bin/env python3
import os
import random

btrfs_path='/var/mocker'
import btrfsutil


def mocker_check(uuid):
    with btrfsutil.SubvolumeIterator('/', 256) as it:
        # This is just an example use-case for fileno(). It is not necessary.
        btrfsutil.sync(it.fileno())
        for path, id_ in it:
            print(id_, path)

    it = btrfsutil.SubvolumeIterator('/subvol', info=True, post_order=True)
    try:
        for path, info in it:
            print(info.id, info.parent_id, path)
    finally:
        it.close()


def mocker_init(directory):
    '''
    init <directory> - создать образ контейнера
    используя указанную директорию как корневую.
    Возвращает в stdout id созданного образа.
    '''
    #uuid="img_$(shuf -i 42002-42254 -n 1)"
    #img ???????????????????????????????
    uuid = random.randint(42002, 42254)
    if os.path.exists(directory):
        if mocker_check(uuid) == 0:
            mocker_run(directory)#???????????????? vse argumenti v stolbik

            #btrfs subvolume create "$btrfs_path/$uuid" > /dev/null
            #cp -rf --reflink=auto "$1"/* "$btrfs_path/$uuid" > /dev/null
            #[[ ! -f "$btrfs_path/$uuid"/img.source ]] && echo "$1" > "$btrfs_path/$uuid"/img.source
            if os.path.exists(btrfs_path + str(uuid)):
                pass
            print("created " + str(uuid))
        pass
    else:
        print("Noo directory named " + directory + " exists")
    pass


def mocker_pull():
    '''
    pull <image> - скачать последний (latest)
    тег указанного образа с Docker Hub.
    Возвращает в stdout id созданного образа.
    '''
    pass


def mocker_rmi():
    '''
    rmi <image_id> - удаляет
    ранее созданный образ из локального хранилища.
    '''
    pass


def mocker_rm():
    '''
    rm <container_id> - удаляет ранее
    созданный контейнер
    '''
    pass


def mocker_images():
    '''
    images - выводит список локальный образов
    '''
    pass


def mocker_ps():
    '''
    ps - выводит список контейнеров
    '''
    pass


def mocker_run():
    '''
    run <image_id> <command> - создает контейнер
    из указанного image_id и запускает его
    с указанной командой
    '''
    pass


def mocker_exec():
    '''
    exec <container_id> <command> - запускает
    указанную команду внутри уже запущенного
    указанного контейнера
    '''
    pass


def mocker_logs():
    '''
    logs <container_id> - выводит логи
    указанного контейнера
    '''
    pass


def mocker_commit():
    '''
    commit <container_id> <image_id> - создает новый
    образ, применяя изменения из образа
    container_id к образу image_id
    '''
    pass


def mocker_help():
    '''
    help - выводит help по командам
    '''
    pass

mocker_check(100)
