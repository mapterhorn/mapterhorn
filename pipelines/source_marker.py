# Download completeness marker for source-store/{source}/.
#
# DOWNLOAD_COMPLETE is written only after every URL has been fetched
# successfully. It is removed when a download starts, so an interrupted
# run never looks finished. wget --continue still resumes partial files.
import os

import utils

MARKER_NAME = 'DOWNLOAD_COMPLETE'


def source_folder(source):
    return utils.store_dir('source-store') + '/{}'.format(source)


def marker_path(source):
    return source_folder(source) + '/{}'.format(MARKER_NAME)


def is_marker_filename(name):
    return name == MARKER_NAME or name == '{}.tmp'.format(MARKER_NAME)


def is_download_complete(source):
    return os.path.isfile(marker_path(source))


def clear_download_marker(source):
    path = marker_path(source)
    tmp = path + '.tmp'
    if os.path.isfile(path):
        os.remove(path)
    if os.path.isfile(tmp):
        os.remove(tmp)


def begin_download(source):
    utils.create_folder(source_folder(source))
    clear_download_marker(source)


def mark_download_complete(source):
    folder = source_folder(source)
    utils.create_folder(folder)
    path = marker_path(source)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        f.write('ok\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def require_download_complete(source):
    if is_download_complete(source):
        return
    raise RuntimeError(
        'source {} is not fully downloaded (missing {}). '
        'Run: just manage autodownload {}'.format(source, MARKER_NAME, source)
    )


def has_bounds(source):
    return os.path.isfile(source_folder(source) + '/bounds.csv')


def is_source_ready(source):
    return is_download_complete(source) and has_bounds(source)
