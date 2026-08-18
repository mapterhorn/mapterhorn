# Source-store markers.
#
# DOWNLOAD_COMPLETE: every URL has been fetched (wget --continue may still
# have been used). Removed when a download starts. Unzip/convert may still
# be running.
#
# READY: catalog Justfile finished (unzip, cog, bounds, polygonize, tarball).
# Written only at the end. Covering and the downloader require this file.
import os

import utils

DOWNLOAD_MARKER = 'DOWNLOAD_COMPLETE'
READY_MARKER = 'READY'


def source_folder(source):
    return utils.store_dir('source-store') + '/{}'.format(source)


def marker_path(source):
    return source_folder(source) + '/{}'.format(DOWNLOAD_MARKER)


def ready_path(source):
    return source_folder(source) + '/{}'.format(READY_MARKER)


def is_marker_filename(name):
    names = (
        DOWNLOAD_MARKER,
        '{}.tmp'.format(DOWNLOAD_MARKER),
        READY_MARKER,
        '{}.tmp'.format(READY_MARKER),
    )
    return name in names


def _atomic_write(path, body='ok\n'):
    tmp = path + '.tmp'
    folder = os.path.dirname(path)
    utils.create_folder(folder)
    with open(tmp, 'w') as f:
        f.write(body)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _clear_file(path):
    tmp = path + '.tmp'
    if os.path.isfile(path):
        os.remove(path)
    if os.path.isfile(tmp):
        os.remove(tmp)


def is_download_complete(source):
    return os.path.isfile(marker_path(source))


def is_source_ready(source):
    return os.path.isfile(ready_path(source))


def clear_download_marker(source):
    _clear_file(marker_path(source))


def clear_ready_marker(source):
    _clear_file(ready_path(source))


def begin_download(source):
    utils.create_folder(source_folder(source))
    clear_download_marker(source)
    clear_ready_marker(source)


def begin_extract(source):
    # Unzip/convert in progress: must not look ready.
    clear_ready_marker(source)


def mark_download_complete(source):
    _atomic_write(marker_path(source))


def mark_ready(source):
    _atomic_write(ready_path(source))


def require_download_complete(source):
    if is_download_complete(source):
        return
    raise RuntimeError(
        'source {} is not fully downloaded (missing {}). '
        'Run: just manage autodownload {}'.format(source, DOWNLOAD_MARKER, source)
    )


def require_ready(source):
    if is_source_ready(source):
        return
    raise RuntimeError(
        'source {} is not READY (download/extract still in progress). '
        'Run: just manage autodownload {}'.format(source, source)
    )


def has_bounds(source):
    return os.path.isfile(source_folder(source) + '/bounds.csv')
