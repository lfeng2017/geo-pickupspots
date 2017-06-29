#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import arrow
from fabric.api import *
from fabric.context_managers import *
from fabric.contrib.console import confirm
from fabric.colors import red, green

# 登录用户和主机名：
env.user = 'lujin'
env.hosts = ['124.250.26.88']

TARGERT_SERV = "172.17.0.57"
# TARGERT_SERV = "172.17.2.48"
INSTALL_DIR = "/home/lujin/jobs/"
PKG = "deploy.tar.gz"
PROJECT = "PickupSpotsMiner"


@task
def pack():
    ' 定义一个pack任务 '
    tar_files = ['*.py', 'config/*', 'dict/*', 'etl/*', 'out', 'log', 'sql/*',
                 'strategy/*', 'common/*', 'BaseMiner/*', 'IncrementalMiner/*', 'requirements.txt']
    local('rm -f {pkg}'.format(pkg=PKG))
    local('tar -czvf {pkg} --exclude=\'*.tar.gz *.pyc fabfile.py test.* *.log *.txt\' {files}'
          .format(pkg=PKG, files=' '.join(tar_files)))
    print "[INFO] 打包完成!", PKG


@task
def deploy():
    ' 标准部署任务: 备份+替换 '

    server = "{user}@{serv}".format(user=env.user, serv=TARGERT_SERV)

    # step 1. 通过跳板机发送至目标服务器
    # 远程服务器的临时文件
    remote_tmp_tar = '~/{pkg}'.format(pkg=PKG)
    run('rm -f %s' % remote_tmp_tar)
    # 上传tar文件至跳板机
    put(PKG, remote_tmp_tar)
    # 跳板机传递至目标机器
    pkg = os.path.join(INSTALL_DIR, PKG)
    run('ssh -t -p 22 {server} "rm -f {pkg}"'.format(server=server, pkg=pkg))
    run('scp {tar} {server}:{install_dir}'.format(tar=remote_tmp_tar, server=server, install_dir=INSTALL_DIR))
    print "[INFO] 上传完成:", PKG

    # 备份老项目
    tag = arrow.now().format("YYYYMMDD")
    bak_tar_name = "{proj}_{tag}.tar.gz".format(proj=PROJECT, tag=tag)
    bak_tar = os.path.join(INSTALL_DIR, bak_tar_name)
    proj_folder = os.path.join(INSTALL_DIR, PROJECT)
    print proj_folder
    run('ssh -t -p 22 {server} "rm -f {old_tar}"'.format(server=server, old_tar=bak_tar))
    run('ssh -t -p 22 {server} "if [ -d {folder} ]; then tar -zcvf {tar} {folder}; fi"' \
        .format(server=server, tar=bak_tar, folder=proj_folder))
    print "[INFO] 备份完成:", bak_tar

    # 解压并替换老项目
    run('ssh -t -p 22 {server} "rm -fr {folder}"'.format(server=server, folder=proj_folder))
    run('ssh -t -p 22 {server} "mkdir -p {folder}"'.format(server=server, folder=proj_folder))
    run('ssh -t -p 22 {server} "tar -zxvf {pkg} -C {folder}/"'.format(server=server, pkg=pkg, folder=proj_folder))
    run('ssh -t -p 22 {server} "rm {pkg}"'.format(server=server, pkg=pkg))
    print "[INFO] 替换完成:", PROJECT


@task
def deployTest():
    ' 测试环境部署'
    with cd(INSTALL_DIR):
        print(red("delete exists dir ..."))
        run("rm -fr {}".format(PROJECT))
        run("mkdir {}".format(PROJECT))
        print(green("put and extract ..."))
        put(PKG, PKG)
        run("tar -zxvf {} -C {}/".format(PKG, PROJECT))
        run("rm -fr {}".format(PKG))

    print "[INFO] 部署完成:", PROJECT
