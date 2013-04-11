import os
from subprocess import Popen, PIPE

def compiler_flag(f):
    return "-%s" % f

def pkg_config(what):
    bufsize, pkg_config_cmd = 1024, "%s --cflags --libs %s" % (pkg_config.path, what)
    f = Popen(pkg_config_cmd, shell=True, bufsize=bufsize, stdout=PIPE).stdout
    data = f.read().strip().split()
    ret = f.close()
    if ret:
        if ret/256: data = []
        if ret/256 > 1:
            raise EnvironmentError("%s not found" % (pkg_config.path,))
    return data

pkg_config.path = "pkg-config"

def pkg_cflags_libs(pkgname):
    cflags_libs = pkg_config(pkgname)
    libraries = [x[2:] for x in cflags_libs if x.startswith(compiler_flag("l"))]
    library_dirs = [x[2:] for x in cflags_libs if x.startswith(compiler_flag("L"))]
    include_dirs = [x[2:] for x in cflags_libs if x.startswith(compiler_flag("I"))]
    return (libraries, library_dirs, include_dirs)

def get_config():
    dirname = "src"
    sources = [os.path.join(dirname, x) for x in os.listdir(dirname) if x.endswith(".c")]
    libraries = []
    library_dirs = []
    include_dirs = []

    a,b,c = pkg_cflags_libs("glib-2.0")
    libraries.extend(a)
    library_dirs.extend(b)
    include_dirs.extend(c)

    a,b,c = pkg_cflags_libs("python")
    if not a and not b and not c:
        a = ["python2.7"]
        b = ["/opt/apps_install/python-sce-2.7.1/lib"]
        c = ["/opt/apps_install/python-sce-2.7.1/include/python2.7"]
    libraries.extend(a)
    library_dirs.extend(b)
    include_dirs.extend(c)

    options = dict(
        sources = sources,
        libraries = libraries,
        library_dirs = library_dirs,
        include_dirs = include_dirs,
        )
    return options
