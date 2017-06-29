#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import arrow
from fabric.api import *

# 登录用户和主机名：
env.user = 'lujin'
env.hosts = ['XX.XX.XX.XX']

TARGERT_SERV = "XX.XX.XX.XX"
INSTALL_DIR = "~/jobs/"
PKG = "deploy.tar.gz"
PROJECT = "application"


def pack():
    ' 定义一个pack任务 '
    tar_files = ['{}/*'.format(PROJECT), 'requirements.txt', 'init_pkg.sh'
                 '*.py', 'config/*', 'deploy/*', 'tests/*', 'data', 'log']

    local('rm -f {pkg}'.format(pkg=PKG))
    local('tar -czvf {pkg} --exclude=\'*.tar.gz fabfile.py *.pyc\' {files}'
          .format(pkg=PKG, files=' '.join(tar_files)))
    print "[INFO] 打包完成!", PKG


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
    run('scp {tar} {server}:{install_dir}'.format(
        tar=remote_tmp_tar, server=server, install_dir=INSTALL_DIR))
    print "[INFO] 上传完成:", PKG

    # 备份老项目
    tag = arrow.now().format("YYYYMMDD")
    bak_name = "{proj}_{tag}.tar.gz".format(proj=PROJECT, tag=tag)
    bak = os.path.join(INSTALL_DIR, bak_name)
    proj_folder = os.path.join(INSTALL_DIR, PROJECT)
    run('rm -f {bak}'.format(bak=bak))
    run('if [ -d {folder} ]; then tar -zcvf {tar} {folder}; fi'.format(tar=bak, folder=proj_folder))
    print "[INFO] 备份完成:", bak

    # 解压并替换老项目
    run('rm -fr {folder}'.format(folder=proj_folder))
    run('mkdir -p {folder}'.format(folder=proj_folder))
    run('tar -zxvf {pkg} -C {folder}/'.format(pkg=pkg, folder=proj_folder))
    run('rm {pkg}'.format(pkg=pkg))
    print "[INFO] 替换完成:", PROJECT
