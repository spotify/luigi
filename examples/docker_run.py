from __future__ import absolute_import

import luigi
import luigi.contrib
import luigi.contrib.docker


class DockerHelloWorld(luigi.contrib.docker.DockerTask):

    @property
    def command(self):
        return [{
            'command': ['echo', 'hello world'],
        }]

if __name__ == '__main__':
    luigi.build([DockerHelloWorld(image='alpine:3.1', remove=True, remove_volumes=True)], local_scheduler=True)
