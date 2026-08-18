# Clear and load source / shoreline data for the Mapterhorn pipeline.
#
# Examples (run from pipelines/):
#   uv run python source_manage.py list
#   uv run python source_manage.py clear gebco --yes
#   uv run python source_manage.py autodownload -y
#   uv run python source_manage.py autodownload gebco emodnet -y
#   uv run python source_manage.py mark-complete ukengland
#   uv run python source_manage.py reload --ocean --yes
#   uv run python source_manage.py clear-shoreline --yes
#   uv run python source_manage.py load-shoreline
import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from glob import glob

import utils
import log
import source_marker
from source_download import catalog_urls

CATALOG_ROOT = utils.catalog_root()
PIPELINES_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_RECIPE_NEEDLES = (
    'source_download.py',
    'source_gmrt_download.py',
    'create_file_list.py',
)


def catalog_sources():
    return sorted([
        path.rstrip('/').split('/')[-2]
        for path in glob(utils.catalog_path('*', 'metadata.json'))
    ])


def loaded_sources():
    return sorted([
        path.rstrip('/').split('/')[-1]
        for path in glob(utils.store_dir('source-store') + '/*/')
    ])


def source_metadata(source):
    path = utils.catalog_path(source, 'metadata.json')
    if not os.path.isfile(path):
        return None
    with open(path) as f:
        return json.load(f)


def dir_size_bytes(path):
    total = 0
    if not os.path.isdir(path):
        return 0
    for root, _, files in os.walk(path):
        for name in files:
            fp = os.path.join(root, name)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total


def format_bytes(n):
    units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
    size = float(n)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == 'B':
                return '{} {}'.format(int(size), unit)
            return '{:.1f} {}'.format(size, unit)
        size /= 1024.0


def count_rasters(source):
    folder = utils.store_dir('source-store') + '/{}'.format(source)
    return len(glob('{}/*.tif'.format(folder)) + glob('{}/*.tiff'.format(folder)) + glob('{}/*.nc'.format(folder)))


def expected_urls(source):
    list_path = utils.catalog_path(source, 'file_list.txt')
    if not os.path.isfile(list_path):
        return None
    with open(list_path) as f:
        return sum(1 for line in f if line.strip() and not line.strip().startswith('#'))


def resolve_sources(names, ocean_only=False, land_only=False, all_loaded=False):
    if all_loaded:
        names = loaded_sources()
    if not names and (ocean_only or land_only):
        names = catalog_sources()

    resolved = []
    for name in names:
        meta = source_metadata(name)
        domain = (meta or {}).get('domain', 'land')
        if ocean_only and domain not in ('ocean', 'both'):
            continue
        if land_only and domain not in ('land', 'both'):
            continue
        if domain == 'mask' and not all_loaded:
            # shoreline is managed via clear-shoreline / load-shoreline
            continue
        resolved.append(name)
    return resolved


def confirm(prompt, assume_yes):
    if assume_yes:
        return True
    answer = input('{} [y/N] '.format(prompt)).strip().lower()
    return answer in ('y', 'yes')


def paths_for_source(source, derived=True):
    paths = [utils.store_dir('source-store') + '/{}'.format(source)]
    if derived:
        paths.extend([
            utils.store_dir('polygon-store') + '/{}.gpkg'.format(source),
            utils.store_dir('tar-store') + '/{}.tar'.format(source),
            utils.store_dir('meta-store') + '/tar/{}.json'.format(source),
        ])
    return paths


def clear_paths(paths, dry_run=False):
    removed = []
    for path in paths:
        if not (os.path.isfile(path) or os.path.isdir(path)):
            continue
        removed.append(path)
        if dry_run:
            print('  would remove {}'.format(path))
            continue
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        print('  removed {}'.format(path))
    return removed


def cmd_list(args):
    print('{:<18} {:<8} {:>8} {:>10} {:>8} {:>8} {}'.format(
        'SOURCE', 'DOMAIN', 'TIFS', 'EXPECTED', 'DL', 'READY', 'SIZE'))
    print('-' * 80)
    seen = set()
    for source in sorted(set(loaded_sources()) | set(catalog_sources())):
        meta = source_metadata(source)
        if meta is None and source not in loaded_sources():
            continue
        domain = (meta or {}).get('domain', 'land')
        if args.ocean and domain not in ('ocean', 'both'):
            continue
        if args.land and domain not in ('land', 'both'):
            continue
        if domain == 'mask':
            continue
        folder = utils.store_dir('source-store') + '/{}'.format(source)
        loaded = os.path.isdir(folder)
        tifs = count_rasters(source) if loaded else 0
        expected = expected_urls(source)
        downloaded = source_marker.is_download_complete(source)
        ready = source_marker.is_source_ready(source)
        size = format_bytes(dir_size_bytes(folder)) if loaded else '-'
        print('{:<18} {:<8} {:>8} {:>10} {:>8} {:>8} {}'.format(
            source,
            domain,
            tifs if loaded else '-',
            expected if expected is not None else '-',
            'yes' if downloaded else ('no' if loaded else '-'),
            'yes' if ready else ('no' if loaded else '-'),
            size if loaded else '(not loaded)',
        ))
        seen.add(source)

    shoreline = utils.store_dir('mask-store') + '/shoreline'
    if os.path.isdir(shoreline):
        ready = os.path.isfile('{}/READY'.format(shoreline)) or os.path.isfile('{}/land_3857.gpkg'.format(shoreline))
        print()
        print('shoreline mask: {} ({})'.format(
            'ready' if ready else 'partial',
            format_bytes(dir_size_bytes(shoreline)),
        ))
    else:
        print()
        print('shoreline mask: (not prepared)')


def cmd_clear(args):
    sources = resolve_sources(
        args.sources,
        ocean_only=args.ocean,
        land_only=args.land,
        all_loaded=args.all,
    )
    if not sources:
        print('no sources matched')
        return 1

    print('Will clear {} source(s): {}'.format(len(sources), ', '.join(sources)))
    if not args.dry_run and not confirm('Proceed?', args.yes):
        print('aborted')
        return 1

    for source in sources:
        print('clearing {}...'.format(source))
        clear_paths(paths_for_source(source, derived=not args.keep_derived), dry_run=args.dry_run)
        log.info('cleared source', source=source)
    return 0


def run_source_justfile(source, dry_run=False):
    catalog_just = '{}/{}/Justfile'.format(CATALOG_ROOT, source)
    if not os.path.isfile(catalog_just):
        raise FileNotFoundError('missing Justfile for source {}'.format(source))
    cmd = ['just', '{}/{}/'.format(CATALOG_ROOT, source)]
    print('running: {}'.format(' '.join(cmd)))
    if dry_run:
        return
    subprocess.check_call(cmd)


_PRINT_LOCK = threading.Lock()
_STATUS = None
_SPIN_FRAMES = '⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
_HIDE_CURSOR = '\033[?25l'
_SHOW_CURSOR = '\033[?25h'
_CLEAR_LINE = '\r\033[2K'


def _color_enabled(stream):
    if os.environ.get('NO_COLOR'):
        return False
    return hasattr(stream, 'isatty') and stream.isatty()


def _paint(text, code, enabled):
    if not enabled:
        return text
    return '\033[{}m{}\033[0m'.format(code, text)


def _short_step(text):
    text = (text or '').strip()
    if text.startswith('running: '):
        for part in text.split():
            if part.endswith('.py'):
                name = os.path.basename(part)
                if name.startswith('source_'):
                    name = name[len('source_'):]
                if name.endswith('.py'):
                    name = name[:-3]
                return name.replace('_', ' ')
        return 'run'
    if text.startswith('[') and ']' in text[:24]:
        return text.split(']', 1)[0] + ']'
    if len(text) > 36:
        return text[:35] + '…'
    return text


class AutoStatus:
    def __init__(self, total, verbose=False, stream=None):
        self.total = max(0, int(total))
        self.verbose = bool(verbose)
        self.stream = stream or sys.stderr
        self.live = self.stream.isatty()
        self.color = _color_enabled(self.stream)
        self.succeeded = 0
        self.failed = 0
        self.downloading = set()
        self.preparing = set()
        self.latest = {}
        self.touched = {}
        self._spin = 0
        self._closed = False
        self._plain_key = None
        self._seq = 0

    def begin(self, kind, source):
        with _PRINT_LOCK:
            if kind == 'download':
                self.preparing.discard(source)
                self.downloading.add(source)
            else:
                self.downloading.discard(source)
                self.preparing.add(source)
            self.latest[source] = kind
            self._touch(source)
            if self.live:
                self._draw()

    def note(self, kind, source, text):
        with _PRINT_LOCK:
            self.latest[source] = text
            self._touch(source)
            if self.verbose:
                self._write_log('[{} {}] {}'.format(kind, source, text))
            elif self.live:
                self._draw()

    def stage_done(self, source, kind):
        with _PRINT_LOCK:
            if kind == 'download':
                self.downloading.discard(source)
            else:
                self.preparing.discard(source)
            if self.live:
                self._draw()

    def source_done(self, source, ok):
        with _PRINT_LOCK:
            self.downloading.discard(source)
            self.preparing.discard(source)
            if ok:
                self.succeeded += 1
            else:
                self.failed += 1
            if self.live:
                self._draw()
            else:
                self._draw_plain()

    def tick(self):
        with _PRINT_LOCK:
            if self._closed or not self.live:
                return
            self._spin += 1
            self._draw()

    def println(self, text):
        with _PRINT_LOCK:
            self._write_log(text)

    def close(self):
        with _PRINT_LOCK:
            if self._closed:
                return
            self._closed = True
            if self.live:
                self.stream.write(_CLEAR_LINE)
                self.stream.write(_SHOW_CURSOR)
                self.stream.flush()

    def _touch(self, source):
        self._seq += 1
        self.touched[source] = self._seq

    def _write_log(self, text):
        if self.live:
            self.stream.write(_CLEAR_LINE)
            self.stream.flush()
        sys.stdout.write(text + '\n')
        sys.stdout.flush()
        if self.live:
            self._draw()

    def _counts(self):
        done = self.succeeded + self.failed
        active = len(self.downloading) + len(self.preparing)
        queued = max(0, self.total - done - active)
        return done, queued

    def _status_left(self, compact):
        done, queued = self._counts()
        spin = _paint(
            _SPIN_FRAMES[self._spin % len(_SPIN_FRAMES)],
            '36',
            self.color,
        )
        succeeded = _paint(
            '{} {}'.format(self.succeeded, 'ok' if compact else 'succeeded'),
            '32',
            self.color and self.succeeded > 0,
        )
        failed = _paint(
            '{} {}'.format(self.failed, 'fail' if compact else 'failed'),
            '31',
            self.color and self.failed > 0,
        )
        if compact:
            return '{}  {}/{} done  {}  {}  {} dl  {} prep  {} queued'.format(
                spin, done, self.total, succeeded, failed,
                len(self.downloading), len(self.preparing), queued,
            )
        return '{}  {}/{} done  ·  {}  ·  {}  ·  {} downloading  ·  {} preparing  ·  {} queued'.format(
            spin, done, self.total, succeeded, failed,
            len(self.downloading), len(self.preparing), queued,
        )

    def _jobs_text(self):
        names = list(self.downloading) + list(self.preparing)
        names.sort(key=lambda name: self.touched.get(name, 0), reverse=True)
        bits = []
        shown = names[:3]
        for name in shown:
            bits.append('{} {}'.format(name, _short_step(self.latest.get(name, ''))))
        extra = len(names) - len(shown)
        if extra > 0:
            bits.append('+{}'.format(extra))
        return ' · '.join(bits)

    def _visible_len(self, text):
        n = 0
        i = 0
        while i < len(text):
            if text[i] == '\033':
                end = text.find('m', i)
                if end == -1:
                    break
                i = end + 1
                continue
            n += 1
            i += 1
        return n

    def _draw(self):
        if self._closed or not self.live:
            return
        self.stream.write(_HIDE_CURSOR)
        width = max(40, shutil.get_terminal_size(fallback=(120, 24)).columns)
        left = self._status_left(compact=False)
        if self._visible_len(left) > width - 10:
            left = self._status_left(compact=True)
        jobs = ''
        if not self.verbose:
            jobs = self._jobs_text()
        line = left
        if jobs:
            sep = '  │  '
            room = width - self._visible_len(left) - len(sep)
            if room >= 8:
                if len(jobs) > room:
                    jobs = jobs[:max(0, room - 1)] + '…'
                line = left + sep + jobs
        if self._visible_len(line) > width:
            line = self._status_left(compact=True)
        self.stream.write(_CLEAR_LINE)
        self.stream.write(line)
        self.stream.flush()

    def _draw_plain(self):
        done, queued = self._counts()
        key = (done, self.succeeded, self.failed, len(self.downloading), len(self.preparing), queued)
        if key == self._plain_key:
            return
        self._plain_key = key
        sys.stdout.write(
            'autodownload  {}/{} done  ·  {} succeeded  ·  {} failed  ·  {} downloading  ·  {} preparing  ·  {} queued\n'.format(
                done, self.total, self.succeeded, self.failed,
                len(self.downloading), len(self.preparing), queued,
            )
        )
        sys.stdout.flush()


def emit(stage, source, text):
    text = (text or '').rstrip()
    if text == '':
        return
    if _STATUS is not None:
        _STATUS.note(stage, source, text)
        return
    with _PRINT_LOCK:
        print('[{} {}] {}'.format(stage, source, text), flush=True)


def run_prefixed(cmd, stage, source):
    emit(stage, source, 'running: {}'.format(' '.join(cmd)))
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    env['UV_NO_SYNC'] = '1'
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=PIPELINES_DIR,
        env=env,
        bufsize=0,
    )
    buf = b''
    while True:
        chunk = p.stdout.read(4096)
        if not chunk:
            break
        buf += chunk
        text = buf.decode('utf-8', errors='replace').replace('\r', '\n')
        lines = text.split('\n')
        buf = lines[-1].encode('utf-8')
        for line in lines[:-1]:
            emit(stage, source, line)
    if buf:
        emit(stage, source, buf.decode('utf-8', errors='replace'))
    code = p.wait()
    if code != 0:
        raise subprocess.CalledProcessError(code, cmd)


def py_cmd(*args):
    return [sys.executable] + list(args)


def justfile_recipe_lines(source):
    just_path = '{}/{}/Justfile'.format(CATALOG_ROOT, source)
    if not os.path.isfile(just_path):
        raise FileNotFoundError('missing Justfile for source {}'.format(source))
    lines = []
    with open(just_path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('#') or line.startswith('['):
                continue
            if line.endswith(':') and ' ' not in line:
                continue
            if ' #' in line:
                line = line.split(' #', 1)[0].rstrip()
            lines.append(line)
    return lines


def is_download_recipe_line(line):
    return any(needle in line for needle in DOWNLOAD_RECIPE_NEEDLES)


def recipe_line_to_cmd(line):
    store = utils.store_dir('source-store').rstrip('/') + '/'
    line = line.replace('source-store/', store)
    parts = line.split()
    if len(parts) >= 4 and parts[:3] == ['uv', 'run', 'python']:
        return py_cmd(*parts[3:])
    if len(parts) >= 2 and parts[0] == 'python':
        return py_cmd(*parts[1:])
    return ['bash', '-lc', line]


def source_download_cmd(source):
    just_path = '{}/{}/Justfile'.format(CATALOG_ROOT, source)
    text = ''
    if os.path.isfile(just_path):
        with open(just_path) as f:
            text = f.read()
    if 'source_gmrt_download.py' in text:
        return py_cmd('source_gmrt_download.py', source)
    if 'source_download.py' in text or catalog_urls(source):
        return py_cmd('source_download.py', source)
    return None


def autodownload_one_download(source, force):
    if _STATUS is not None:
        _STATUS.begin('download', source)
    if force:
        source_marker.clear_download_marker(source)
        source_marker.clear_ready_marker(source)
    if source_marker.is_download_complete(source) and not force:
        emit('download', source, 'already fetched, skip wget')
        return
    cmd = source_download_cmd(source)
    if cmd is None:
        emit('download', source, 'no download step')
        return
    run_prefixed(cmd, 'download', source)
    if not source_marker.is_download_complete(source):
        if catalog_urls(source):
            raise RuntimeError(
                '{} finished without {}'.format(source, source_marker.DOWNLOAD_MARKER))
        source_marker.mark_download_complete(source)
    emit('download', source, 'download complete')


def autodownload_one_prep(source):
    if _STATUS is not None:
        _STATUS.begin('prep', source)
    cmds = [
        recipe_line_to_cmd(line)
        for line in justfile_recipe_lines(source)
        if not is_download_recipe_line(line)
    ]
    if not cmds:
        emit('prep', source, 'no unzip/bounds steps in Justfile')
    for cmd in cmds:
        run_prefixed(cmd, 'prep', source)
    if not source_marker.is_download_complete(source):
        if catalog_urls(source):
            raise RuntimeError(
                '{} finished without {}'.format(source, source_marker.DOWNLOAD_MARKER))
        source_marker.mark_download_complete(source)
    source_marker.mark_ready(source)
    emit('prep', source, 'READY')
    log.info('autodownload source', source=source)


def autodownload_shoreline(force):
    if _STATUS is not None:
        _STATUS.begin('prep', 'shoreline')
    if force:
        ready = utils.store_dir('mask-store') + '/shoreline/READY'
        if os.path.isfile(ready):
            os.remove(ready)
    cmd = [sys.executable, 'source_prepare_shoreline.py']
    run_prefixed(cmd, 'prep', 'shoreline')
    log.info('loaded shoreline')


def cmd_load(args):
    sources = resolve_sources(
        args.sources,
        ocean_only=args.ocean,
        land_only=args.land,
    )
    if not sources:
        print('no sources matched — pass source names, or --ocean / --land')
        return 1

    print('Will load {} source(s): {}'.format(len(sources), ', '.join(sources)))
    if not args.dry_run and not confirm('Proceed? This may download a lot of data.', args.yes):
        print('aborted')
        return 1

    for source in sources:
        if source_metadata(source) is None:
            print('skip unknown catalog source: {}'.format(source))
            continue
        if not args.force and source_marker.is_source_ready(source):
            print('skip {} (already READY)'.format(source))
            continue
        if args.force and not args.dry_run:
            source_marker.clear_download_marker(source)
            source_marker.clear_ready_marker(source)
        print('loading {}...'.format(source))
        run_source_justfile(source, dry_run=args.dry_run)
        if not args.dry_run:
            source_marker.mark_ready(source)
        log.info('loaded source', source=source)
    return 0


def cmd_reload(args):
    sources = resolve_sources(
        args.sources,
        ocean_only=args.ocean,
        land_only=args.land,
        all_loaded=args.all,
    )
    if not sources:
        print('no sources matched')
        return 1

    print('Will reload {} source(s): {}'.format(len(sources), ', '.join(sources)))
    print('  1) clear source-store (+ derived unless --keep-derived)')
    print('  2) run catalog Justfile (download + prep)')
    if not args.dry_run and not confirm('Proceed?', args.yes):
        print('aborted')
        return 1

    for source in sources:
        if source_metadata(source) is None:
            print('skip unknown catalog source: {}'.format(source))
            continue
        print('reloading {}...'.format(source))
        clear_paths(paths_for_source(source, derived=not args.keep_derived), dry_run=args.dry_run)
        run_source_justfile(source, dry_run=args.dry_run)
        if not args.dry_run:
            source_marker.mark_ready(source)
        log.info('reloaded source', source=source)
    return 0


def cmd_clear_shoreline(args):
    paths = [utils.store_dir('mask-store') + '/shoreline']
    print('Will clear shoreline mask store')
    if not args.dry_run and not confirm('Proceed?', args.yes):
        print('aborted')
        return 1
    clear_paths(paths, dry_run=args.dry_run)
    log.info('cleared shoreline')
    return 0


def cmd_load_shoreline(args):
    print('Will prepare shoreline mask (S2Coast + GSHHG)')
    if not args.dry_run and not confirm('Proceed? Large download/rasterize.', args.yes):
        print('aborted')
        return 1
    if args.force and not args.dry_run:
        clear_paths([utils.store_dir('mask-store') + '/shoreline'], dry_run=False)
    cmd = [sys.executable, 'source_prepare_shoreline.py']
    print('running: {}'.format(' '.join(cmd)))
    if not args.dry_run:
        subprocess.check_call(cmd)
        log.info('loaded shoreline')
    return 0


def autodownload_catalog_sources(include_debug=False):
    names = []
    for name in catalog_sources():
        if name.startswith('debug-') and not include_debug:
            continue
        meta = source_metadata(name) or {}
        if meta.get('domain') == 'mask':
            continue
        names.append(name)
    return names


def shoreline_is_ready():
    shoreline = utils.store_dir('mask-store') + '/shoreline'
    return (
        os.path.isfile('{}/READY'.format(shoreline))
        and os.path.isfile('{}/land_3857.gpkg'.format(shoreline))
    )


def cmd_autodownload(args):
    explicit = list(args.sources or [])
    if explicit or args.ocean or args.land:
        sources = resolve_sources(
            args.sources,
            ocean_only=args.ocean,
            land_only=args.land,
        )
        if not args.include_debug:
            sources = [s for s in sources if (not s.startswith('debug-') or s in explicit)]
    else:
        sources = autodownload_catalog_sources(include_debug=args.include_debug)

    if not sources and not args.sources and not args.ocean and not args.land:
        print('no catalog sources found')
        return 1

    named_only = bool(explicit)
    do_shoreline = (not args.skip_shoreline) and (not named_only or 's2coast' in explicit)

    skip_ready = []
    skip_nourl = []
    to_run = []
    for source in sources:
        if source == 's2coast':
            continue
        if source_metadata(source) is None:
            print('skip unknown catalog source: {}'.format(source))
            continue
        if args.force:
            urls = catalog_urls(source)
            if not urls and source not in explicit:
                skip_nourl.append(source)
            else:
                to_run.append(source)
            continue
        if source_marker.is_source_ready(source):
            skip_ready.append(source)
            continue
        urls = catalog_urls(source)
        if not urls:
            folder = source_marker.source_folder(source)
            if source in explicit and os.path.isdir(folder):
                to_run.append(source)
            else:
                skip_nourl.append(source)
            continue
        to_run.append(source)

    print('autodownload: {} to fetch/prepare, {} already complete, {} manual/no URLs'.format(
        len(to_run), len(skip_ready), len(skip_nourl)))
    print('  workers: {} download, {} prep'.format(args.jobs, args.prep_jobs))
    if do_shoreline:
        if not args.force and shoreline_is_ready():
            print('  shoreline: already ready')
            do_shoreline = False
        else:
            print('  shoreline: will prepare')
    if args.verbose:
        if to_run:
            print('  fetch: {}'.format(', '.join(to_run)))
        if skip_ready:
            print('  skip complete: {}'.format(', '.join(skip_ready)))
        if skip_nourl:
            print('  skip (no file_list URLs; ingest manually then mark-complete): {}'.format(
                ', '.join(skip_nourl)))
    elif skip_nourl:
        print('  skip (no file_list URLs): {}'.format(', '.join(skip_nourl)))

    if not to_run and not do_shoreline:
        print('nothing to do')
        return 0

    if args.dry_run:
        print('dry-run: not fetching (download jobs={}, prep jobs={})'.format(
            args.jobs, args.prep_jobs))
        return 0

    if not confirm(
        'Proceed? {} download workers, {} prep workers. Incomplete downloads resume.'.format(
            args.jobs, args.prep_jobs),
        args.yes,
    ):
        print('aborted')
        return 1

    if args.jobs > 1:
        os.environ['MAPTERHORN_WGET_QUIET'] = '1'
    os.environ['UV_NO_SYNC'] = '1'
    os.environ['PYTHONUNBUFFERED'] = '1'

    failures = []
    download_jobs = max(1, args.jobs)
    prep_jobs = max(1, args.prep_jobs)
    verbose = bool(args.verbose)

    already_dl = []
    need_dl = []
    for source in to_run:
        if source_marker.is_download_complete(source) and not args.force:
            already_dl.append(source)
        else:
            need_dl.append(source)
    need_dl.sort(key=lambda name: (len(catalog_urls(name)), name))

    total = len(to_run) + (1 if do_shoreline else 0)
    global _STATUS
    _STATUS = AutoStatus(total, verbose=verbose)

    def record_failure(name, err):
        failures.append((name, str(err)))
        _STATUS.source_done(name, ok=False)
        if verbose:
            _STATUS.println('FAILED {}: {}'.format(name, err))

    try:
        with ThreadPoolExecutor(max_workers=download_jobs) as download_ex, \
                ThreadPoolExecutor(max_workers=prep_jobs) as prep_ex:
            pending = {}

            def submit_prep(name, fn, *fn_args):
                fut = prep_ex.submit(fn, *fn_args)
                pending[fut] = ('prep', name)

            if do_shoreline:
                submit_prep('shoreline', autodownload_shoreline, args.force)
            for source in already_dl:
                submit_prep(source, autodownload_one_prep, source)
            for source in need_dl:
                fut = download_ex.submit(autodownload_one_download, source, args.force)
                pending[fut] = ('download', source)

            _STATUS.tick()
            timeout = 0.08 if _STATUS.live else 1.0
            while pending:
                done, _ = wait(
                    list(pending),
                    timeout=timeout,
                    return_when=FIRST_COMPLETED,
                )
                _STATUS.tick()
                for fut in done:
                    kind, name = pending.pop(fut)
                    try:
                        fut.result()
                    except Exception as e:
                        record_failure(name, e)
                        continue
                    if kind == 'download':
                        _STATUS.stage_done(name, 'download')
                        submit_prep(name, autodownload_one_prep, name)
                    else:
                        _STATUS.source_done(name, ok=True)
    except KeyboardInterrupt:
        _STATUS.println('interrupted')
        raise
    finally:
        _STATUS.close()
        _STATUS = None

    if failures:
        print('autodownload finished with {} failure(s):'.format(len(failures)))
        for name, err in failures:
            print('  {}: {}'.format(name, err))
        return 1
    print('autodownload finished  {}/{} succeeded'.format(total, total))
    return 0


def cmd_mark_complete(args):
    for source in args.sources:
        folder = source_marker.source_folder(source)
        if not os.path.isdir(folder):
            print('skip {}: no directory {}'.format(source, folder))
            continue
        source_marker.mark_download_complete(source)
        source_marker.mark_ready(source)
        print('wrote {} and {}'.format(
            source_marker.marker_path(source),
            source_marker.ready_path(source),
        ))
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        description='Clear and load Mapterhorn source / shoreline data',
    )
    sub = parser.add_subparsers(dest='command', required=True)

    p_list = sub.add_parser('list', help='show catalog vs loaded sources')
    p_list.add_argument('--ocean', action='store_true', help='only ocean/both domain')
    p_list.add_argument('--land', action='store_true', help='only land/both domain')
    p_list.set_defaults(func=cmd_list)

    p_clear = sub.add_parser('clear', help='delete loaded source data')
    p_clear.add_argument('sources', nargs='*', help='source ids')
    p_clear.add_argument('--all', action='store_true', help='all currently loaded sources')
    p_clear.add_argument('--ocean', action='store_true', help='ocean/both domain sources')
    p_clear.add_argument('--land', action='store_true', help='land/both domain sources')
    p_clear.add_argument('--keep-derived', action='store_true', help='keep polygon/tar/meta')
    p_clear.add_argument('--yes', '-y', action='store_true', help='do not prompt')
    p_clear.add_argument('--dry-run', action='store_true')
    p_clear.set_defaults(func=cmd_clear)

    p_load = sub.add_parser('load', help='download + prepare sources via catalog Justfile')
    p_load.add_argument('sources', nargs='*', help='source ids')
    p_load.add_argument('--ocean', action='store_true')
    p_load.add_argument('--land', action='store_true')
    p_load.add_argument('--yes', '-y', action='store_true')
    p_load.add_argument('--force', action='store_true', help='re-run even if already complete')
    p_load.add_argument('--dry-run', action='store_true')
    p_load.set_defaults(func=cmd_load)

    p_reload = sub.add_parser('reload', help='clear then load sources')
    p_reload.add_argument('sources', nargs='*', help='source ids')
    p_reload.add_argument('--all', action='store_true', help='reload all currently loaded sources')
    p_reload.add_argument('--ocean', action='store_true')
    p_reload.add_argument('--land', action='store_true')
    p_reload.add_argument('--keep-derived', action='store_true')
    p_reload.add_argument('--yes', '-y', action='store_true')
    p_reload.add_argument('--dry-run', action='store_true')
    p_reload.set_defaults(func=cmd_reload)

    p_cs = sub.add_parser('clear-shoreline', help='delete mask-store/shoreline')
    p_cs.add_argument('--yes', '-y', action='store_true')
    p_cs.add_argument('--dry-run', action='store_true')
    p_cs.set_defaults(func=cmd_clear_shoreline)

    p_ls = sub.add_parser('load-shoreline', help='run source_prepare_shoreline.py')
    p_ls.add_argument('--force', action='store_true', help='clear existing shoreline first')
    p_ls.add_argument('--yes', '-y', action='store_true')
    p_ls.add_argument('--dry-run', action='store_true')
    p_ls.set_defaults(func=cmd_load_shoreline)

    p_auto = sub.add_parser(
        'autodownload',
        help='download/prepare all (or named) sources, skipping already-complete ones',
    )
    p_auto.add_argument('sources', nargs='*', help='source ids (default: all catalog sources)')
    p_auto.add_argument('--ocean', action='store_true')
    p_auto.add_argument('--land', action='store_true')
    p_auto.add_argument('--skip-shoreline', action='store_true')
    p_auto.add_argument('--include-debug', action='store_true', help='include debug-* catalog sources')
    p_auto.add_argument('--force', action='store_true', help='ignore READY/DOWNLOAD_COMPLETE and re-fetch')
    p_auto.add_argument('--jobs', '-j', type=int, default=32, help='parallel download workers (default 32)')
    p_auto.add_argument(
        '--prep-jobs',
        type=int,
        default=8,
        help='parallel unzip/bounds/tarball workers (default 8)',
    )
    p_auto.add_argument('--yes', '-y', action='store_true')
    p_auto.add_argument('--verbose', '-v', action='store_true', help='print each job step instead of a side snippet')
    p_auto.add_argument('--dry-run', action='store_true')
    p_auto.set_defaults(func=cmd_autodownload)

    p_mc = sub.add_parser(
        'mark-complete',
        help='write DOWNLOAD_COMPLETE and READY for a manually ingested source',
    )
    p_mc.add_argument('sources', nargs='+', help='source ids')
    p_mc.set_defaults(func=cmd_mark_complete)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if args.command == 'list':
        return args.func(args) or 0
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main() or 0)
