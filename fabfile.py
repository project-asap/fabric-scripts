import os, sys

from fabric.api import cd, env, run, sudo
from fabric.contrib.files import exists

env.hosts = ["localhost"]

ASAP_HOME = "%s/asap" % os.environ['HOME']

WF_HOME = "%s/workflow" % ASAP_HOME
WF_REPO = "https://github.com/project-asap/workflow.git"

IRES_HOME = "%s/IReS-Platform" % ASAP_HOME
IRES_REPO = "https://github.com/project-asap/IReS-Platform.git"

SPARK_HOME = "%s/Spark-Nested" % ASAP_HOME
SPARK_REPO = "https://github.com/project-asap/Spark-Nested.git"
SPARK_BRANCH = "nested-hierarchical"

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
    try :
        run("mvn -v")
    except :
        sudo("apt-get install maven")

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

def check_for_yarn():
    try:
        HADOOP_PREFIX = os.environ['HADOOP_PREFIX']
    except KeyError:
        print "Exiting...you should install hadoop/yarn first"
        sys.exit(-1)
    else:
        HADOOP_VERSION = run("%s/bin/yarn version|head -1|cut -d ' ' -f 2" % HADOOP_PREFIX)
    return HADOOP_PREFIX, HADOOP_VERSION

def clone_IReS():
    if not exists(IRES_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % IRES_REPO)

def start_IReS():
    with cd(IRES_HOME):
        run_script = "asap-platform/asap-server/src/main/scripts/asap-server"
        run("%s start" % run_script)

def stop_IReS():
    with cd(IRES_HOME):
        run_script = "asap-platform/asap-server/src/main/scripts/asap-server"
        run("%s stop" % run_script)

def bootstrap_IReS():
    install_mvn()

    clone_IReS()

    with cd(IRES_HOME):
        # Conditional build
        if not exists("asap-platform/asap-server/target"):
            for d in ("cloudera-kitten", "panic", "asap-platform"):
                with cd(d):
                    run("mvn clean install -DskipTests")

        # Set IRES_HOME in asap-server script
        run_script = "asap-platform/asap-server/src/main/scripts/asap-server"
        c = run("grep \"^IRES_HOME=\" %s | wc -l" % run_script)
        if (c == "0"): # only if it is not already set
            run("sed -i '/#$IRES_HOME=$/a\IRES_HOME=%s' %s" % (IRES_HOME,
                                                            run_script))
        HADOOP_PREFIX, _ = check_for_yarn()
        for f in ("core-site.xml", "yarn-site.xml"):
            sudo("cp %s/etc/hadoop/%s "
                "asap-platform/asap-server/target/conf/" % (HADOOP_PREFIX, f))

    start_IReS()


def clone_spark():
    if not exists(SPARK_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % SPARK_REPO)

def bootstrap_spark():
    clone_spark()

    with cd(SPARK_HOME):
        _, HADOOP_VERSION = check_for_yarn()
        run("git checkout hierR")
        run("./sbt/sbt -Dhadoop.version=%s -Pyarn -DskipTests clean assembly" %
            HADOOP_VERSION)

def bootstrap():

    if not exists(ASAP_HOME):
        run("mkdir -p %s" % ASAP_HOME)
    bootstrap_wf()
    bootstrap_IReS()
    bootstrap_spark()
#    bootstrap_operators()
#    bootstrap_telecom_analytics()
#    bootstrap_web_analytics()


def remove_wf():
    stop_nginx()

    #TODO Add prompt
    uninstall_nginx()

    run("rm -rf %s" % WF_HOME)

    uninstall_grunt()
    uninstall_npm()

def remove():
    remove_wf()
