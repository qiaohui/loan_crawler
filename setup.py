#!/usr/bin/env python

from distutils.core import setup
from script import __version__ as version
from setuptools import find_packages

try:
    long_description = open("README.md").read()
except IOError:
    long_description = ""

# PRE REQUIRED
# sudo apt-get install -y --force-yes libxml2-dev libxslt1-dev libjpeg-dev libpng-dev python-mysqldb
setup(
    name='loan_crawler',
    version=version,
    description='loan common python libs',
    long_description=long_description,
    author='qiaohui.zhang',
    author_email='qiaohui.zhang@gmail.com',
    url='',
    packages=find_packages(exclude=[]),
    exclude_package_data={'': []},
    data_files=[],
    scripts = ['script/loan.py',
               'script/91wangcai.py',
               'script/jimubox.py',
               'script/ppdai.py',
               'script/yinhu.py',
               'script/eloancn.py',
               'script/lufax.py',
               'script/tzydb.py',
               'script/xinhehui.py',
               'script/renrendai.py',
               'script/yirendai.py',
               'script/licaifan.py',
               'script/iqianbang.py',
               'script/touna.py',
               'script/my089.py',
               'script/qian360.py',
               'script/xiaomabank.py',
               'script/id68.py',
               'script/weidai.py',
               'script/niwodai.py',
               'script/he-pai.py',
               'script/tuandai.py',
               'script/longlongweb.py',
               ],
    license="8741.cn",
    dependency_links=[],
    install_requires = ["sqlalchemy",
                        "redis",
                        "pymongo",
                        "zc-zookeeper-static",
                        "pykeeper",
                        "daemon",
                        "python-gflags",
                        "simplejson",
                        "jinja2",
                        "lxml",
                        "PIL",
                        "web.py",
                        "BeautifulSoup",
                        "python-dateutil",
                        "urllib3",
                        "requests",
                        "pyTOP",
                        "pygaga",
                        ],
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'License :: 8741.cn',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
# sudo easy_install dateutils requests urllib3

