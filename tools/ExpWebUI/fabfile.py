#!/usr/bin/env python
# -*- coding: utf-8 -*-

import arrow
import config as cfg
from fabric.api import *

# 登录用户和主机名：
env.user = 'lujin'
env.hosts = ['124.250.26.88']

TARGERT_SERV = "172.17.0.57"
PKG = "deploy.tar.gz"
PROJECT = cfg.APP


def pack():
	' 定义一个pack任务 '
	tar_files = ['*.py', 'common/*', 'models/*', 'sql/*', 'static/*', 'templates/*', 'views/*', 'log/']
	local('rm -f {pkg}'.format(pkg=PKG))
	local('tar -czvf {pkg} --exclude=\'*.tar.gz\' --exclude=\'fabfile.py\' {files}'
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
	run('scp {tar} {server}:~/'.format(tar=remote_tmp_tar, server=server))
	print "[INFO] 上传完成:", PKG

	# 备份老项目
	tag = arrow.now().format("YYYYMMDD")
	bak_name = "{proj}_{tag}.tar.gz".format(proj=PROJECT, tag=tag)
	run('ssh -t -p 22 {server} "rm -f {bak}"'.format(server=server, bak=bak_name))
	run('ssh -t -p 22 {server} "if [ -d {proj} ]; then tar -zcvf {bak} {proj}; fi"'.format(server=server, bak=bak_name, proj=PROJECT))
	print "[INFO] 备份完成:", bak_name

	# 解压并替换老项目
	run('ssh -t -p 22 {server} "rm -fr {proj}"'.format(server=server, proj=PROJECT))
	run('ssh -t -p 22 {server} "mkdir {proj}"'.format(server=server, proj=PROJECT))
	run('ssh -t -p 22 {server} "tar -zxvf {pkg} -C {proj}/"'.format(server=server, proj=PROJECT, pkg=PKG))
	run('ssh -t -p 22 {server} "rm {pkg}"'.format(server=server, proj=PROJECT, pkg=PKG))
	print "[INFO] 替换完成:", PROJECT
