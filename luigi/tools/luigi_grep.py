#!/usr/bin/env python

import argparse
import json
from collections import defaultdict

from luigi import six
from luigi.six.moves.urllib.request import urlopen


class LuigiGrep(object):

    def __init__(self, host, port):
        self._host = host
        self._port = port

    @property
    def graph_url(self):
        return "http://{0}:{1}/api/graph".format(self._host, self._port)

    def _fetch_json(self):
        """Returns the json representation of the dep graph"""
        print("Fetching from url: " + self.graph_url)
        resp = urlopen(self.graph_url).read()
        return json.loads(resp.decode('utf-8'))

    def _build_results(self, jobs, job):
        job_info = jobs[job]
        deps = job_info['deps']
        deps_status = defaultdict(list)
        for j in deps:
            if j in jobs:
                deps_status[jobs[j]['status']].append(j)
            else:
                deps_status['UNKNOWN'].append(j)
        return {"name": job, "status": job_info['status'], "deps_by_status": deps_status}

    def prefix_search(self, job_name_prefix):
        """searches for jobs matching the given job_name_prefix."""
        json = self._fetch_json()
        jobs = json['response']
        for job in jobs:
            if job.startswith(job_name_prefix):
                yield self._build_results(jobs, job)

    def status_search(self, status):
        """searches for jobs matching the given status"""
        json = self._fetch_json()
        jobs = json['response']
        for job in jobs:
            job_info = jobs[job]
            if job_info['status'].lower() == status.lower():
                yield self._build_results(jobs, job)


def main():
    parser = argparse.ArgumentParser(
        "luigi-grep is used to search for workflows using the luigi scheduler's json api")
    parser.add_argument(
        "--scheduler-host", default="localhost", help="hostname of the luigi scheduler")
    parser.add_argument(
        "--scheduler-port", default="8082", help="port of the luigi scheduler")
    parser.add_argument("--prefix", help="prefix of a task query to search for", default=None)
    parser.add_argument("--status", help="search for jobs with the given status", default=None)

    args = parser.parse_args()
    grep = LuigiGrep(args.scheduler_host, args.scheduler_port)

    results = []
    if args.prefix:
        results = grep.prefix_search(args.prefix)
    elif args.status:
        results = grep.status_search(args.status)

    for job in results:
        print("{name}: {status}, Dependencies:".format(name=job['name'], status=job['status']))
        for (status, jobs) in six.iteritems(job['deps_by_status']):
            print("  status={status}".format(status=status))
            for job in jobs:
                print("    {job}".format(job=job))


if __name__ == '__main__':
    main()
