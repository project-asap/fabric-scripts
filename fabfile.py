import os, sys
import time

from functools import wraps

from fabric.api import cd, env, parallel, roles, run, sudo, execute
from fabric.context_managers import quiet, shell_env
from fabric.contrib.files import exists
from fabric.decorators import task
from fabric.operations import prompt

from socket import gethostname

env.hosts = ["localhost"]
env.roledefs = {
    'spark_nodes': []  # define the IPs for the spark nodes
}
env.roledefs['spark_master'] = env.roledefs['spark_nodes'][0:-1]

ASAP_HOME = "%s/asap" % os.environ['HOME']

WMT_HOME = "%s/workflow" % ASAP_HOME
WMT_REPO = "https://github.com/project-asap/workflow.git"
WMT_PORT = "8888"

HOSTNAME = gethostname()

IRES_HOME = "%s/IReS-Platform" % ASAP_HOME
IRES_REPO = "https://github.com/project-asap/IReS-Platform.git"

SPARK_HOME = "%s/spark01" % ASAP_HOME
SPARK_REPO = "https://github.com/project-asap/spark01.git"
SPARK_BRANCH = "distScheduling"
def _get_local_ip():
    r = run("ifconfig $1 | grep \"inet addr\" | gawk -F: '{print $2}' | gawk '{print $1}'")
    return [ip for ip in r.split() if ip != "127.0.0.1"][0]
SPARK_MASTER = env.roledefs['spark_master'][0] \
    if len(env.roledefs['spark_master']) >= 1 \
    else _get_local_ip()

SPARK_TESTS_HOME = "%s/spark-tests" % ASAP_HOME
SPARK_TESTS_REPO = "https://github.com/project-asap/spark-tests.git"

SWAN_HOME = "%s/swan" % ASAP_HOME
SWAN_LLVM_REPO = "https://github.com/project-asap/swan_llvm.git"
SWAN_CLANG_REPO = "https://github.com/project-asap/swan_clang.git"
SWAN_RT_REPO = "https://github.com/project-asap/swan_runtime.git"

SBT_VERSION = "0.13.11"

VHOST = "asap"

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


def ping_service(url, status):
    with quiet():
        return run('curl -s -o /dev/null -w "%%{http_code}" %s' % url) == status

def wait_until(somepredicate, timeout=100, period=0.25, *args, **kwargs):
    t = time.time()
    mustend = t + timeout
    while time.time() < mustend:
        if somepredicate(*args, **kwargs): return
        time.sleep(period)

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
def install_php_fpm():
    sudo('apt-get install php-fpm')

@task
@acknowledge('Do you want to remove php-fpm?')
def uninstall_php_fpm():
    sudo('apt-get purge php-fpm')

@task
@acknowledge('Do you want to remove grant?')
def uninstall_grunt():
    sudo("npm uninstall -g grunt-cli")

@task
def config_wmt():
    sites_available = "/etc/nginx/sites-available/%s" % VHOST
    sites_enabled = "/etc/nginx/sites-enabled/%s" % VHOST
    with cd(WMT_HOME):
        sudo("cp wmt.conf.default %s" % sites_available)
        sudo("sed -ri \"s/(listen)(.*)(;)/\\1\\t%s\\3/\" %s" % (WMT_PORT, sites_available))
        sudo("sed -ri \"s/\/Users\/max\/Projects\/workflow/%s/\" %s" %
                ('\/'.join(WMT_HOME.split('/')), sites_available))
        if exists(sites_enabled):
            sudo("rm %s" % sites_enabled)
        sudo("ln -s %s %s" % (sites_available, sites_enabled))

@task
def install_nginx():
    sudo("apt-get install nginx")
    config_wmt()

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
    sudo("apt-get purge maven")

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
    with cd(os.path.join(WMT_HOME, 'pub/py')):
        sudo('apt-get install python-ruamel.yaml')
        run('python -m unittest -v testmain')
    content = run("curl http://localhost:%s" % WMT_PORT)
    assert("workflow" in content)

@task
def bootstrap_wmt():
    install_npm()
    install_php_fpm()
    install_grunt()
    install_wmt()
    install_nginx()
    start_nginx()
    test_wmt()

def check_for_yarn():
    try:
        HADOOP_PREFIX = os.environ['HADOOP_PREFIX']
    except KeyError:
        print("Exiting...you should install hadoop/yarn first")
        sys.exit(-1)
    else:
        HADOOP_VERSION = run("%s/bin/yarn version|head -1|cut -d ' ' -f 2" % HADOOP_PREFIX)
    return HADOOP_PREFIX, HADOOP_VERSION

def clone_IReS():
    if not exists(IRES_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % IRES_REPO)

def start_IReS_new():
    with cd(IRES_HOME):
        run('./install.sh -r start')

@task
def start_IReS():
    with cd(IRES_HOME):
        with shell_env(ASAP_SERVER_HOME='%s' % os.path.join(IRES_HOME, 'asap-platform/asap-server/target')):
            run("nohup ./asap-platform/asap-server/src/main/scripts/asap-server start")


def stop_IReS_new():
    with cd(IRES_HOME):
        run('./install.sh -r stop')

@task
def stop_IReS():
    with cd(IRES_HOME):
        with shell_env(ASAP_SERVER_HOME='%s' % os.path.join(IRES_HOME, 'asap-platform/asap-server/target')):
            run("./asap-platform/asap-server/src/main/scripts/asap-server stop")



@task
def test_IReS():
    wait_until(ping_service, url='http://localhost:1323', status='200')
    with shell_env(ASAP_HOME='%s' % IRES_HOME):
        with cd(IRES_HOME):
            for d in ("panic", "cloudera-kitten", "asap-platform"):
                with cd(d):
                    run("mvn -Dmaven.test.failure.ignore verify")
    with cd("%s/asap-platform/asap-client" % IRES_HOME):
        for eg in ("TestOperators", "TestWorkflows"):
            run("mvn exec:java -Dexec.mainClass="
                "\"gr.ntua.cslab.asap.examples.%s\"" % eg)

def bootstrap_IReS_old():
    def build():
        # Conditional build
        if not exists("asap-platform/asap-server/target"):
            for d in ("panic", "cloudera-kitten", "asap-platform"):
                with cd(d):
                    run("mvn clean install -DskipTests")

    install_mvn()

    clone_IReS()

    with cd(IRES_HOME):
        # Temporary hack for solving temporary issues with inner dependencies
        with quiet():
            build()
        build()
        # Update hadoop version
        HADOOP_PREFIX, HADOOP_VERSION = check_for_yarn()
        for f in ('asap-platform/pom.xml', 'cloudera-kitten/pom.xml'):
            change_xml_property("hadoop.version", HADOOP_VERSION, f)
        for f in ("core-site.xml", "yarn-site.xml"):
            sudo("cp %s/etc/hadoop/%s "
                "asap-platform/asap-server/target/conf/" % (HADOOP_PREFIX, f))
    start_IReS()
    test_IReS()

@task
def install_IReS():
    clone_IReS()
    with cd(IRES_HOME):
        HADOOP_PREFIX, _ = check_for_yarn()
        run('./install.sh')

@task
def bootstrap_IReS():
    install_mvn()
    install_IReS()
    start_IReS()
    test_IReS()

def clone_spark():
    if not exists(SPARK_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % SPARK_REPO)


def clone_spark_tests():
    if not exists(SPARK_TESTS_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % SPARK_TESTS_REPO)

@task
@roles('spark_master')
def start_spark():
    with cd(SPARK_HOME):
        run("./sbin/start-all.sh")


@task
@roles('spark_master')
def stop_spark():
    with cd(SPARK_HOME):
        run("./sbin/stop-all.sh")

@task
@roles('spark_master')
def build_spark_tests():
    install_sbt()

    with cd(SPARK_TESTS_HOME):
        library_path = os.path.join(SPARK_TESTS_HOME, 'lib')

        # copy spark-assembly jar to the library
        run("mkdir -p %s" % library_path)
        run("cp %s/assembly/target/scala-2.10/spark-assembly-*.jar lib/" % SPARK_HOME)
        run("sbt clean package")

@task
@roles('spark_master')
def test_spark_hierarchical():
    with cd(SPARK_TESTS_HOME):
        run("%s/bin/spark-submit --class HierarchicalKMeansPar "
            "target/scala-2.10/spark-tests_2.10-1.0.jar spark://%s:7077 "
            "100 2 2 2 file:///%s/data/hierRDD/test0.txt --dist-sched false" %
            (SPARK_HOME, SPARK_MASTER, SPARK_TESTS_HOME))

@task
@roles('spark_master')
def test_spark_distributed_scheduler():
    with cd(SPARK_TESTS_HOME):
        run("%s/bin/spark-submit --class Run "
        "target/scala-2.10/spark-tests_2.10-1.0.jar "
        "--master spark://%s:7077 --algo Filter33 --dist-sched true "
        "--nsched 4 --partitions 32 --runs 15" %
        (SPARK_HOME, SPARK_MASTER))

@task
@roles('spark_master')
def test_spark():
    execute(clone_spark_tests)

    execute(build_spark_tests)

    #execute(test_nested_map)
    execute(test_spark_hierarchical)
    execute(test_spark_distributed_scheduler)

@task
def install_sbt():
    try:
        run("sbt help")
    except:
        sbt_url = 'https://dl.bintray.com/sbt/debian'
        try:
            run("grep %s /etc/apt/sources.list.d/sbt.list" % sbt_url)
        except:
            run("echo \"deb https://dl.bintray.com/sbt/debian /\" | sudo tee -a /etc/apt/sources.list.d/sbt.list")
        sudo("apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823")
        sudo("apt-get update")
        sudo("apt-get install sbt")

@task
@acknowledge('Do you want to remove sbt?')
def uninstall_sbt():
    sudo("apt-get purge sbt")


@task
@parallel
@roles('spark_nodes')
def build_spark():
    clone_spark()

    with cd(SPARK_HOME):
        _, HADOOP_VERSION = check_for_yarn()
        run("git checkout %s" % SPARK_BRANCH)

        # Change sbt version causing IllegalStateException https://github.com/sbt/sbt/issues/2015
        run("sed -i \"/sbt.version=/ s/=.*/=%s/\" project/build.properties" % SBT_VERSION)

        run("./build/sbt -Dhadoop.version=%s -Pyarn -DskipTests clean assembly"
             % HADOOP_VERSION)


@task
@parallel
@roles('spark_nodes')
def configure_spark():
    with cd(SPARK_HOME):
        run("cp conf/spark-env.sh.template conf/spark-env.sh")
        run("sed -i '/SPARK_MASTER_IP/a SPARK_MASTER_IP=%s' conf/spark-env.sh" % SPARK_MASTER)
        run("cp conf/spark-defaults.conf.template conf/spark-defaults.conf")
        if env.host == SPARK_MASTER:
            run("cp conf/slaves.template conf/slaves")
            run("sed -i '/localhost/a %s' conf/slaves" % '\\n'.join(env.roledefs['spark_nodes']))
            run("sed -i '/localhost/d' conf/slaves")


@task
def bootstrap_spark():
    execute(build_spark)
    execute(configure_spark)
    execute(start_spark)
    execute(test_spark)

@task
def install_cmake():
    sudo('apt-get install cmake')

@task
@acknowledge('Do you want to remove cmake?')
def uninstall_cmake():
    sudo("apt-get purge cmake")

@task
def install_libnumadev():
    sudo('apt-get install libnuma-dev')

@task
@acknowledge('Do you want to remove libnuma-dev?')
def uninstall_libnumadev():
    sudo("apt-get purge libnuma-dev")
@task
def test_clang():
    with cd(SWAN_HOME):
        with shell_env(PATH='$PATH:%s/build/bin' % SWAN_HOME):
            run('clang --help')
            run('clang++ --help')
            run('clang llvm/utils/count/count.c -fsyntax-only')
            run('clang llvm/utils/count/count.c -S -emit-llvm -o -')
            run('clang llvm/utils/count/count.c -S -emit-llvm -o - -O3')
            run('clang llvm/utils/count/count.c -S -O3 -o -')

@task
def bootstrap_swan():
    install_cmake()
    install_libnumadev()

    run('mkdir -pp %s' % SWAN_HOME)

    with cd(SWAN_HOME):
        if (not exists(os.path.join(SWAN_HOME, 'llvm'))):
            run("git clone %s llvm" % SWAN_LLVM_REPO)
        run('mkdir -p llvm/tools/clang')
        if (not exists(os.path.join(SWAN_HOME, 'llvm/tools/clang'))):
            run("git clone %s llvm/tools/clang" % SWAN_CLANG_REPO)
        run('mkdir -p build')
        with cd('build'):
            run('cmake -G "Unix Makefiles" ../llvm')
            run('make clean')
            run('make')
        test_clang()
        if (not exists(os.path.join(SWAN_HOME, 'swan_runtime'))):
            run("git clone %s" % SWAN_RT_REPO)
        with cd('swan_runtime'):
            run("libtoolize")
            run("aclocal")
            run("automake --add-missing")
            run("autoconf")
            run("./configure --prefix=%s/swan_runtime/lib CC=../build/bin/clang CXX=../build/bin/clang++" % SWAN_HOME)
            run("make clean")
            run("make")
        run("git clone https://github.com/project-asap/swan_tests.git")
        with cd("swan_tests"):
            run("make CXX=../build/bin/clang++ SWANRTDIR=../swan_runtime test")

@task
def remove_swan():
    uninstall_cmake()
    uninstall_libnumadev()
    run("rm -rf %s" % SWAN_HOME)

@task
def bootstrap():

    if not exists(ASAP_HOME):
        run("mkdir -p %s" % ASAP_HOME)
    bootstrap_wmt()
    bootstrap_IReS()
    execute(bootstrap_spark)
    bootstrap_swan()
#    bootstrap_operators()
#    bootstrap_telecom_analytics()
#    bootstrap_web_analytics()


@task
def remove_wmt():
    stop_nginx()
    uninstall_nginx()
    run("rm -rf %s" % WMT_HOME)
    uninstall_grunt()
    uninstall_php_fpm()
    uninstall_npm()

@task
def remove_IReS():
    with quiet():
        stop_IReS()
    run("rm -rf %s" % IRES_HOME)
    uninstall_mvn()

@task
@parallel
@roles('spark_nodes')
def remove_spark():
    if env.host == SPARK_MASTER:
        stop_spark()
    run("rm -rf %s" % SPARK_HOME)
    run("rm -rf %s" % SPARK_TESTS_HOME)

@task
def remove():
    remove_wmt()
    remove_IReS()
    remove_spark()

    run("rm -rf %s" % ASAP_HOME)
