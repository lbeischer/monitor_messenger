# Setup script for app_messenger

from distutils.core import setup
import py2exe, sys, os

sys.setrecursionlimit(5000)

sys.argv.append('py2exe')

setup(
    options = {'py2exe': {
    'bundle_files': 1,
    'includes': ['git+http://github.com/EXASOL/websocket-api.git#egg=exasol-ws-api&subdirectory=python'],
    'excludes': ['six.moves.urllib.parse'],
    }},
    windows = [{'script': "app_monitor_message.py"}],
    zipfile = None,
)