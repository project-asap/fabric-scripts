import os

from fabric.api import cd, env, run, sudo
from fabric.contrib.files import exists

env.hosts = ["localhost"]

ASAP_HOME = "%s/asap" % os.environ['HOME']

WF_HOME = "%s/workflow" % ASAP_HOME
WF_REPO = "https://github.com/project-asap/workflow.git"

IRES_HOME = "%s/IReS-Platform" % ASAP_HOME
IRES_REPO = "https://github.com/project-asap/IReS-Platform.git"

VHOST = "asap"
VHOST_CONFIG = """server {
    listen   8081;

    location / {

    root %s/pub/;
    index  main.html;
    }
}""" % WF_HOME

def install_npm():
    sudo("apt-get install npm")

def uninstall_npm():
    sudo("apt-get purge npm")

def install_grunt():
    # install grunt-cli
    sudo("npm install -g grunt-cli")
    if not exists("/usr/bin/node"):
        # create symbolic link for nodejs
        sudo("ln -s /usr/bin/nodejs /usr/bin/node")

def uninstall_grunt():
    sudo("npm uninstall -g grunt-cli")

def config_nginx():
    sites_available = "/etc/nginx/sites-available/%s" % VHOST
    sites_enabled = "/etc/nginx/sites-enabled/%s" % VHOST
    sudo("echo \"%s\" > %s" % (VHOST_CONFIG, sites_available))
    sudo("ln -s %s %s" % (sites_available, sites_enabled))

def install_nginx():
    sudo("apt-get install nginx")
    config_nginx()

def start_nginx():
    sudo("nginx -s reload")

def stop_nginx():
    sudo("nginx -s stop")

def uninstall_nginx():
    sudo("apt-get purge nginx nginx-common")

def install_mvn():
    sudo("apt-get install maven")
    #TODO check version

def install_wf():
    if not exists(WF_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % WF_REPO)
    with cd(WF_HOME):
        run("npm install")
        run("grunt")

def test_wf():
    content = run("curl http://localhost:8081")
    assert("workflow" in content)

def bootstrap_wf():
    install_npm()
    install_grunt()

    install_wf()

    install_nginx()
    start_nginx()

    test_wf()

def bootstrap_IReS():
    install_mvn()

    if not exists(IRES_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % IRES_REPO)
    for d in ("cloudera-kitten", "panic", "asap-platform"):
        with cd("%s/%s" % (IRES_HOME, d)):
            sudo("mvn clean install -DskipTests")

def bootstrap():
#    bootstrap_hadoop()
#    bootstrap_spark()

    if not exists(ASAP_HOME):
        run("mkdir -p %s" % ASAP_HOME)
    bootstrap_wf()
    bootstrap_IReS()
#    bootstrap_operators()

def remove_wf():
    stop_nginx()

    #TODO Add prompt
    uninstall_nginx()

    run("rm -rf %s" % WF_HOME)

    uninstall_grunt()
    uninstall_npm()

def remove():
    remove_wf()
