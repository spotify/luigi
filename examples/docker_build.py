from __future__ import absolute_import

import luigi
import luigi.contrib
import luigi.contrib.docker
import os


class Dockerfile(luigi.Task):
    context_path = luigi.Parameter(default='.')

    def run(self):
        if not os.path.exists(self.context_path):
            os.makedirs(self.context_path)
        with self.output().open('w') as f:
            f.write('FROM alpine\n')
            f.write('RUN apk update && apk add git\n')

    def output(self):
        return luigi.LocalTarget('%s/Dockerfile' % self.context_path)


class BuildImage(luigi.contrib.docker.DockerImageBuildTask):
    def requires(self):
        return [Dockerfile(context_path=self.path)]

if __name__ == '__main__':
    luigi.build([BuildImage(name='luigi-built-image', path='build/context')], local_scheduler=True)
