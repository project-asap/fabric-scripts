import os, sys
import time

from functools import wraps

from fabric.api import cd, env, parallel, roles, run, sudo, execute, warn_only
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
WMT_BRANCH = 'integration'

HOSTNAME = gethostname()

IRES_HOME = "%s/IReS-Platform" % ASAP_HOME
IRES_REPO = "https://github.com/project-asap/IReS-Platform.git"
IRES_BRANCH = 'project-asap-patch-3'

SPARK_FORTH_REPO = "https://github.com/project-asap/spark01.git"
SPARK_FORTH_HOME = "/".join([ASAP_HOME, SPARK_FORTH_REPO.split('/')[-1].rsplit('.', 1)[0]])
SPARK_FORTH_BRANCH = "final"
def _get_local_ip():
    r = run("ifconfig $1 | grep \"inet addr\" | gawk -F: '{print $2}' | gawk '{print $1}'")
    return [ip for ip in r.split() if ip != "127.0.0.1"][0]
SPARK_MASTER = env.roledefs['spark_master'][0] \
    if len(env.roledefs['spark_master']) >= 1 \
    else _get_local_ip()

SPARK_VERSION = '1.6.0'
SPARK_DOWNLOAD_LINK = 'http://d3kbcqa49mib13.cloudfront.net/spark-%s-bin-without-hadoop.tgz' % SPARK_VERSION
SPARK_HOME = "/".join([ASAP_HOME, SPARK_DOWNLOAD_LINK.split('/')[-1].rsplit('.', 1)[0]])

SPARK_FORTH_TESTS_HOME = "%s/spark-tests" % ASAP_HOME
SPARK_FORTH_TESTS_REPO = "https://github.com/project-asap/spark-tests.git"

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

def install_package(package):
    sudo('apt-get install %s' % package)

def uninstall_package(package):
    @acknowledge('Do you want to remove %s?' % package)
    def uninstall():
        sudo("apt-get purge %s" % package)
    return uninstall()

def install_requirements(requirements=()):
    def wrap(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            map(install_package, requirements)
            func(*args, **kwargs)
        return wrapped
    return wrap

def uninstall_requirements(requirements=()):
    def wrap(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            func(*args, **kwargs)
            map(uninstall_package, requirements)
        return wrapped
    return wrap

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


def upload_to_hdfs(local_path, hdfs_path):
    run('hdfs dfs -put -f %s %s' % (local_path, hdfs_path))

@task
def config_npm():
    user = os.environ['USER']
    group = run("groups|cut -d ' ' -f 1")
    with quiet():
        sudo("chown -fR %s:%s ~/.npm ~/tmp" % (user, group))


@task
def config_grunt():
    if not exists("/usr/bin/node"):
        # create symbolic link for nodejs
        sudo("ln -s /usr/bin/nodejs /usr/bin/node")


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
def start_nginx():
    sudo('/etc/init.d/nginx restart')

@task
@acknowledge('Do you want to stop nginx?')
def stop_nginx():
    with quiet():
        sudo('/etc/init.d/nginx stop')


@task
def install_wmt():
    if not exists(WMT_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % WMT_REPO)
    with cd(WMT_HOME):
        run('git checkout %s' % WMT_BRANCH)
        run("npm install")
        run("grunt")

@task
@install_requirements(('python-ruamel.yaml',))
def test_wmt():
    with cd(os.path.join(WMT_HOME, 'pub/py')):
        run('python -m unittest -v testmain')
    content = run("curl http://localhost:%s/main.html" % WMT_PORT)
    assert("workflow" in content)

@task
def bootstrap_postgres():
    install_package('postgresql-contrib')
    install_package('postgresql')

@task
def remove_postgres():
    uninstall_package('postgresql')

@task
@install_requirements(('npm', 'php-fpm', 'nginx'))
def bootstrap_wmt():
    config_npm()
    config_grunt()
    install_wmt()
    config_wmt()
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

@task
def start_IReS():
    with cd(IRES_HOME):
        with shell_env(ASAP_SERVER_HOME='%s' % os.path.join(IRES_HOME, 'asap-platform/asap-server/target')):
            run("nohup ./asap-platform/asap-server/src/main/scripts/asap-server start")
    wait_until(ping_service, url='http://localhost:1323', status='200')

@task
def stop_IReS():
    with cd(IRES_HOME):
        with shell_env(ASAP_SERVER_HOME='%s' % os.path.join(IRES_HOME, 'asap-platform/asap-server/target')):
            run("./asap-platform/asap-server/src/main/scripts/asap-server stop")


@task
def test_IReS():
    with shell_env(ASAP_HOME='%s' % IRES_HOME):
        with cd(IRES_HOME):
            for d in ("panic", "cloudera-kitten", "asap-platform"):
                with cd(d):
                    run("mvn -Dmaven.test.failure.ignore verify")


@task
def run_IReS_examples():
    with quiet():
        start_IReS()
    with cd("%s/asap-platform/asap-client" % IRES_HOME):
        for eg in ("TestOperators", "TestWorkflows", "TestWorkflowsIMR"):
            with warn_only():
                run("mvn exec:java -Dexec.mainClass="
                    "\"gr.ntua.cslab.asap.examples.%s\"" % eg)


@install_requirements(('maven',))
def bootstrap_IReS_old():
    def build():
        # Conditional build
        if not exists("asap-platform/asap-server/target"):
            for d in ("panic", "cloudera-kitten", "asap-platform"):
                with cd(d):
                    run("mvn clean install -DskipTests")

    clone_IReS()

    with cd(IRES_HOME):
        run("git checkout %s" % IRES_BRANCH)
        # Temporary hack for solving temporary issues with inner dependencies
        with quiet():
            build()
        build()
        # Update hadoop version
        HADOOP_PREFIX, HADOOP_VERSION = check_for_yarn()
        for f in ('asap-platform/pom.xml', 'cloudera-kitten/pom.xml'):
            change_xml_property("hadoop.version", HADOOP_VERSION, f)
        for f in ("core-site.xml", "yarn-site.xml"):
            run("ln -s %s/etc/hadoop/%s "
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
@install_requirements(('maven',))
def bootstrap_IReS():
    install_IReS()
    start_IReS()
    test_IReS()
    #run_IReS_examples()

def clone_spark_forth():
    if not exists(SPARK_FORTH_HOME):
        with cd(ASAP_HOME):
            run("git clone %s %s" % (SPARK_FORTH_REPO, SPARK_FORTH_HOME))


def clone_spark_forth_tests():
    if not exists(SPARK_FORTH_TESTS_HOME):
        with cd(ASAP_HOME):
            run("git clone %s" % SPARK_FORTH_TESTS_REPO)

@task
@roles('spark_master')
def start_spark_forth():
    with quiet():
        stop_spark()
    with cd(SPARK_FORTH_HOME):
        run("./sbin/start-all.sh")


@task
@roles('spark_master')
def start_spark():
    with quiet():
        stop_spark_forth()
    with cd(SPARK_HOME):
        run("./sbin/start-all.sh")

@task
@roles('spark_master')
def stop_spark_forth():
    with cd(SPARK_FORTH_HOME):
        run("./sbin/stop-all.sh")

@task
@roles('spark_master')
def stop_spark():
    with cd(SPARK_HOME):
        run("./sbin/stop-all.sh")

@task
@roles('spark_master')
def build_spark_forth_tests():
    install_sbt()

    with cd(SPARK_FORTH_TESTS_HOME):
        library_path = os.path.join(SPARK_FORTH_TESTS_HOME, 'lib')

        # copy spark-assembly jar to the library
        run("mkdir -p %s" % library_path)
        run("cp %s/assembly/target/scala-2.10/spark-assembly-*.jar lib/" % SPARK_FORTH_HOME)
        run("sbt clean package")

@task
@roles('spark_master')
def test_spark_forth_nested():
    with cd(SPARK_FORTH_TESTS_HOME):
        for clss in ('NestedMap1', 'NestedFilter1'):
            run("%s/bin/spark-submit --class %s "
                "target/scala-2.10/spark-tests_2.10-1.0.jar spark://%s:7077" %
                (SPARK_FORTH_HOME, clss, SPARK_MASTER))
@task
@roles('spark_master')
def test_spark_forth_hierarchical():
    with cd(SPARK_FORTH_TESTS_HOME):
        upload_to_hdfs('data/hierRDD/test0.txt', '/tmp/test0.txt')
        run("%s/bin/spark-submit --class HierarchicalKMeansPar "
            "target/scala-2.10/spark-tests_2.10-1.0.jar spark://%s:7077 "
            "100 2 2 2 /tmp/test0.txt --dist-sched false" %
            (SPARK_FORTH_HOME, SPARK_MASTER))

@task
@roles('spark_master')
def test_spark_forth_distributed_scheduler():
    with cd(SPARK_FORTH_TESTS_HOME):
        run("%s/bin/spark-submit --class Run "
        "target/scala-2.10/spark-tests_2.10-1.0.jar "
        "--master spark://%s:7077 --algo Filter33 --dist-sched true "
        "--nsched 4 --partitions 32 --runs 15" %
        (SPARK_FORTH_HOME, SPARK_MASTER))

@task
@roles('spark_master')
def test_spark_forth():
    execute(clone_spark_forth_tests)
    execute(build_spark_forth_tests)
    execute(test_spark_forth_nested)
    execute(test_spark_forth_hierarchical)
    execute(test_spark_forth_distributed_scheduler)

@task
@roles('spark_master')
def test_spark():
    with cd(SPARK_HOME):
        run('MASTER=spark://%s:7077 ./bin/run-example SparkPi' % SPARK_MASTER)

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
@parallel
@roles('spark_nodes')
def build_spark_forth():
    clone_spark_forth()

    with cd(SPARK_FORTH_HOME):
        _, HADOOP_VERSION = check_for_yarn()
        run("git checkout %s" % SPARK_FORTH_BRANCH)

        # Change sbt version causing IllegalStateException https://github.com/sbt/sbt/issues/2015
        run("sed -i \"/sbt.version=/ s/=.*/=%s/\" project/build.properties" % SBT_VERSION)

        run("./build/sbt -Dhadoop.version=%s -Pyarn -DskipTests clean assembly"
             % HADOOP_VERSION)


def configure_spark_basic(spark_dir):
    with cd(spark_dir):
        run("cp conf/spark-env.sh.template conf/spark-env.sh")
        run("sed -i '/SPARK_MASTER_IP/a SPARK_MASTER_IP=%s' conf/spark-env.sh" % SPARK_MASTER)
        # TODO set PYSPARK_PYTHON
        # TODO set HADOOP_CONFDIR
        run("cp conf/spark-defaults.conf.template conf/spark-defaults.conf")
        run("echo 'spark.rpc akka' >> conf/spark-defaults.conf")
        if env.host == SPARK_MASTER:
            run("cp conf/slaves.template conf/slaves")
            run("sed -i '/localhost/a %s' conf/slaves" % '\\n'.join(env.roledefs['spark_nodes']))
            run("sed -i '/localhost/d' conf/slaves")


@task
@parallel
@roles('spark_nodes')
def configure_spark_forth():
    configure_spark_basic(SPARK_FORTH_HOME)

@task
@parallel
@roles('spark_nodes')
def configure_spark():
    configure_spark_basic(SPARK_HOME)
    with cd(SPARK_HOME):
        HADOOP_PREFIX, _ = check_for_yarn()
        hadoop_classpath = run("%s/bin/hadoop classpath" % HADOOP_PREFIX)
        run("echo 'export SPARK_DIST_CLASSPATH=%s' >> conf/spark-env.sh" % hadoop_classpath)
        JAVA_HOME = os.environ['JAVA_HOME']
        run("echo 'export JAVA_HOME=%s' >> conf/spark-env.sh" % JAVA_HOME)


@task
def download_spark():
    with cd(ASAP_HOME):
        tarball = SPARK_DOWNLOAD_LINK.split('/')[-1]
        if (not exists(tarball)):
            run('wget %s' % SPARK_DOWNLOAD_LINK)
        tarball = SPARK_DOWNLOAD_LINK.split('/')[-1]
        run('tar -xvf %s' % tarball)

@task
def bootstrap_spark():
    execute(download_spark)
    execute(configure_spark)
    execute(start_spark)
    execute(test_spark)

@task
def bootstrap_spark_forth():
    execute(build_spark_forth)
    execute(configure_spark_forth)
    execute(start_spark_forth)
    execute(test_spark_forth)

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
@install_requirements(('cmake', 'libnuma-dev', 'libtool', 'm4', 'automake'))
def bootstrap_swan():
    run('mkdir -pp %s' % SWAN_HOME)

    with cd(SWAN_HOME):
        if (not exists(os.path.join(SWAN_HOME, 'llvm'))):
            run("git clone %s llvm" % SWAN_LLVM_REPO)
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
@uninstall_requirements(('cmake', 'libnuma-dev', 'libtool', 'automake'))
def remove_swan():
    run("rm -rf %s" % SWAN_HOME)

@task
def bootstrap():

    if not exists(ASAP_HOME):
        run("mkdir -p %s" % ASAP_HOME)
    bootstrap_postgres
    bootstrap_wmt()
    bootstrap_IReS()
    execute(bootstrap_spark_forth)
    execute(bootstrap_spark)
    bootstrap_swan()
#    bootstrap_operators()
#    bootstrap_telecom_analytics()
#    bootstrap_web_analytics()


@task
@uninstall_requirements(('nginx', 'nginx-common', 'php-fpm', 'npm'))
def remove_wmt():
    stop_nginx()
    run("rm -rf %s" % WMT_HOME)

@task
@uninstall_requirements(('maven',))
def remove_IReS():
    with quiet():
        stop_IReS()
    run("rm -rf %s" % IRES_HOME)

@task
@parallel
@roles('spark_nodes')
def remove_spark_forth():
    if env.host == SPARK_MASTER:
        stop_spark_forth()
    run("rm -rf %s" % SPARK_FORTH_HOME)
    run("rm -rf %s" % SPARK_FORTH_TESTS_HOME)

@task
@parallel
@roles('spark_nodes')
def remove_spark():
    if env.host == SPARK_MASTER:
        stop_spark()
    run("rm -rf %s" % SPARK_HOME)

@task
def remove():
    remove_wmt()
    remove_IReS()
    remove_spark()
    remove_spark_forth()
    remove_swan()
    remove_postgres()

    #run("rm -rf %s" % ASAP_HOME)
