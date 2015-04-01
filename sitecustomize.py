import os
import sys


def patch_process_for_coverage():
    # patch multiprocessing module to get coverage
    # https://bitbucket.org/ned/coveragepy/issue/117/enable-coverage-measurement-of-code-run-by
    from coverage.collector import Collector
    from coverage import coverage
    import multiprocessing
    # detect if coverage was running in forked process

    if sys.version_info >= (3, 4):
        klass = multiprocessing.process.BaseProcess
    else:
        klass = multiprocessing.Process

    if Collector._collectors:
        original = multiprocessing.Process._bootstrap

        class ProcessWithCoverage(multiprocessing.Process):
            def _bootstrap(self):
                cov = coverage(
                    data_suffix=True,
                    config_file=os.getenv('COVERAGE_PROCESS_START', True)
                )
                cov.start()
                try:
                    return original(self)
                finally:
                    cov.stop()
                    cov.save()

    if sys.version_info >= (3, 4):
        klass._bootstrap = ProcessWithCoverage._bootstrap
    else:
        multiprocessing.Process = ProcessWithCoverage


if os.getenv('FULL_COVERAGE', 'false') == 'true':
    try:
        import coverage
        coverage.process_startup()
        patch_process_for_coverage()
    except ImportError:
        pass
