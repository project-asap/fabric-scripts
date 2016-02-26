import os, sys

from functools import wraps

from fabric.api import cd, env, run, sudo
from fabric.context_managers import quiet
from fabric.contrib.files import exists
from fabric.decorators import task
from fabric.operations import prompt

env.hosts = ["localhost"]

ASAP_HOME = "%s/asap" % os.environ['HOME']

WMT_HOME = "%s/workflow" % ASAP_HOME
WMT_REPO = "https://github.com/project-asap/workflow.git"
WMT_PORT = "8888"

IRES_HOME = "%s/IReS-Platform" % ASAP_HOME
IRES_REPO = "https://github.com/project-asap/IReS-Platform.git"

SPARK_HOME = "%s/Spark-Nested" % ASAP_HOME
SPARK_REPO = "https://github.com/project-asap/Spark-Nested.git"
SPARK_BRANCH = "nested-hierarchical"

VHOST = "asap"
VHOST_CONFIG = """server {
    listen   %s;

    location / {

    root %s/pub/;
    index  main.html;
    }
}""" % (WMT_PORT, WMT_HOME)


def yes_or_no(s):
    if s not in ('y', 'n'):
        raise Exception('Just say yes (y) or no (n).')
    return s

def acknowledge(msg):
    def wrap(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            r = prompt('%s [y/N]' % msg, default='n', validate=yes_or_no)
            if r == 'y':
                func(*args, **kwargs)
        return wrapped
    return wrap

@task
def change_xml_property(name, value, file_):
    run("sed -i 's/\(<%s>\)\([^\"]*\)\(<\/%s>\)/\\1%s\\3/g' %s" %
        (name, name, value, file_))

@task
def install_npm():
    try:
        run("npm version")
    except:
        sudo("apt-get install npm")
    user = os.environ['USER']
    group = run("groups|cut -d ' ' -f 1")
    with quiet():
        sudo("chown -fR %s:%s ~/.npm ~/tmp" % (user, group))

@task
@acknowledge('Do you want to remove npm?')
def uninstall_npm():
    sudo("apt-get purge npm")

@task
def install_grunt():
    # install grunt-cli
    sudo("npm install -g grunt-cli")
    if not exists("/usr/bin/node"):
        # create symbolic link for nodejs
        sudo("ln -s /usr/bin/nodejs /usr/bin/node")

@task
@acknowledge('Do you want to remove grant?')
def uninstall_grunt():
    sudo("npm uninstall -g grunt-cli")

@task
def config_nginx():
    sites_available = "/etc/nginx/sites-available/%s" % VHOST
    sites_enabled = "/etc/nginx/sites-enabled/%s" % VHOST
    sudo("echo \"%s\" > %s" % (VHOST_CONFIG, sites_available))
    if not exists(sites_enabled):
        sudo("ln -s %s %s" % (sites_available, sites_enabled))

@task
def install_nginx():
    sudo("apt-get install nginx")
    config_nginx()

@task
def start_nginx():
    sudo("nginx -s reload")

@task
@acknowledge('Do you want to stop nginx?')
def stop_nginx():
    with quiet():
        sudo("nginx -s stop")

@task
@acknowledge('Do you want to remove nginx?')
def uninstall_nginx():
    sudo("apt-get purge nginx nginx-common")

@task
def install_mvn():
    try :
        run("mvn -v")
    except :
        sudo("apt-get install maven")

@task
@acknowledge('Do you want to remove maven?')
def uninstall_mvn():
    sudo("apt-get install maven")

@task
def install_wmt():
    if not exists(WMT_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % WMT_REPO)
    with cd(WMT_HOME):
        run("npm install")
        run("grunt")

@task
def test_wmt():
    content = run("curl http://localhost:%s" % WMT_PORT)
    assert("workflow" in content)

@task
def bootstrap_wmt():
    install_npm()
    install_grunt()
    install_wmt()
    install_nginx()
    start_nginx()
    test_wmt()

def check_for_yarn():
    try:
        HADOOP_PREFIX = os.environ['HADOOP_PREFIX']
    except KeyError:
        print "Exiting...you should install hadoop/yarn first"
        sys.exit(-1)
    else:
        HADOOP_VERSION = run("%s/bin/yarn version|head -1|cut -d ' ' -f 2" % HADOOP_PREFIX)
    return HADOOP_PREFIX, HADOOP_VERSION

@task
def clone_IReS():
    if not exists(IRES_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % IRES_REPO)

@task
def start_IReS():
    with cd(IRES_HOME):
        run_script = "asap-platform/asap-server/src/main/scripts/asap-server"
        run("%s start" % run_script)

@task
def stop_IReS():
    with cd(IRES_HOME):
        run_script = "asap-platform/asap-server/src/main/scripts/asap-server"
        run("%s stop" % run_script)

@task
def test_IReS():
    with cd("%s/asap-platform/asap-client" % IRES_HOME):
        for eg in ("TestOperators", "TestWorkflows"):
            run("mvn exec:java -Dexec.mainClass="
                "\"gr.ntua.cslab.asap.examples.%s\"" % eg)

@task
def bootstrap_IReS():
    def build():
        # Conditional build
        if not exists("asap-platform/asap-server/target"):
            for d in ("panic", "cloudera-kitten", "asap-platform"):
                with cd(d):
                    run("mvn clean install -DskipTests")

    install_mvn()

    clone_IReS()

    with cd(IRES_HOME):
        build()
        # Update hadoop version
        HADOOP_PREFIX, HADOOP_VERSION = check_for_yarn()
        for f in ('asap-platform/pom.xml', 'cloudera-kitten/pom.xml'):
            change_xml_property("hadoop.version", HADOOP_VERSION, f)
        # Set IRES_HOME in asap-server script
        run_script = "asap-platform/asap-server/src/main/scripts/asap-server"
        run("sed -i 's/^\(IRES_HOME\s*=\s*\).*$/\\1%s/' %s" % (IRES_HOME, run_script))
        for f in ("core-site.xml", "yarn-site.xml"):
            sudo("cp %s/etc/hadoop/%s "
                "asap-platform/asap-server/target/conf/" % (HADOOP_PREFIX, f))
    start_IReS()
    test_IReS()

@task
def clone_spark():
    if not exists(SPARK_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % SPARK_REPO)

@task
def bootstrap_spark():
    clone_spark()

    with cd(SPARK_HOME):
        _, HADOOP_VERSION = check_for_yarn()
        run("git checkout hierR")
        run("./sbt/sbt -Dhadoop.version=%s -Pyarn -DskipTests clean assembly" %
            HADOOP_VERSION)

@task
def bootstrap():

    if not exists(ASAP_HOME):
        run("mkdir -p %s" % ASAP_HOME)
    bootstrap_wmt()
    bootstrap_IReS()
    bootstrap_spark()
#    bootstrap_operators()
#    bootstrap_telecom_analytics()
#    bootstrap_web_analytics()


@task
def remove_wmt():
    stop_nginx()
    uninstall_nginx()
    run("rm -rf %s" % WMT_HOME)
    uninstall_grunt()
    uninstall_npm()

@task
def remove_IReS():
    stop_IReS()
    run("rm -rf %s" % IRES_HOME)
    uninstall_mvn()

@task
def remove():
    remove_wmt()
    remove_IReS()

    run("rm -rf %s" % ASAP_HOME)
