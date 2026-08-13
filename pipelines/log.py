# Structured logging for unattended pipeline runs.
from datetime import datetime, timezone
import os
import threading

import utils

_lock = threading.Lock()
_run_id = None
_log_path = None


def get_run_id():
    global _run_id
    if _run_id is None:
        _run_id = os.environ.get('MAPTERHORN_RUN_ID')
        if not _run_id:
            _run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            os.environ['MAPTERHORN_RUN_ID'] = _run_id
    return _run_id


def get_log_path():
    global _log_path
    if _log_path is None:
        utils.create_folder(utils.store_dir('meta-store') + '/logs')
        _log_path = utils.store_dir('meta-store') + '/logs/{}.log'.format(get_run_id())
    return _log_path


def log(level, message, **fields):
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    extra = ''
    if fields:
        parts = ['{}={}'.format(k, v) for k, v in sorted(fields.items())]
        extra = ' ' + ' '.join(parts)
    line = '{} [{}] {}{}'.format(ts, level.upper(), message, extra)
    with _lock:
        print(line, flush=True)
        try:
            with open(get_log_path(), 'a') as f:
                f.write(line + '\n')
        except Exception:
            pass


def info(message, **fields):
    log('info', message, **fields)


def warn(message, **fields):
    log('warn', message, **fields)


def error(message, **fields):
    log('error', message, **fields)
