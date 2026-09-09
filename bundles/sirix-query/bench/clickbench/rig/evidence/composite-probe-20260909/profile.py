"""One baseline diagnostic JVM, with the campaign memory gate and lossless answers."""
import json
from pathlib import Path
import sys

RIG = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RIG))

import measure
import runner


def main():
    original_command = runner.command
    launched = False

    def command(runtime, database, **kwargs):
        nonlocal launched
        if launched:
            raise RuntimeError('profile allowance: one JVM in this invocation')
        memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
        available = int(memory['MemAvailable'].split()[0]) * 1024
        if available < 26 << 30:
            raise RuntimeError(f'busy box: MemAvailable {available} below 26 GiB')
        argv = original_command(runtime, database, **kwargs)
        output = Path(sys.argv[sys.argv.index('--out') + 1]).resolve()
        argv += ['--dump', str(output / 'answers')]
        with (output / 'profile-launch.json').open('x') as stream:
            json.dump(dict(available_bytes=available, argv=argv, profiling_jvms=1,
                           paired_jvms=0, maximum_pairs=12), stream, indent=2)
        launched = True
        return argv

    runner.command = command
    return measure.main()


if __name__ == '__main__':
    sys.exit(main())
