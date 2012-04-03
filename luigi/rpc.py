import urllib
import urllib2
import logging
import time
import json
from scheduler import Scheduler

logger = logging.getLogger('luigi-interface')  # TODO: 'interface'?


class RemoteScheduler(Scheduler):
    ''' Scheduler proxy object. Talks to a RemoteSchedulerResponder '''

    def __init__(self, host='localhost', port=8082):
        self.__host = host
        self.__port = port

    def _request(self, url, data):
        # TODO(erikbern): do POST requests instead
        data = {'data': json.dumps(data)}
        url = 'http://%s:%d%s?%s' % \
            (self.__host, self.__port, url, urllib.urlencode(data))

        req = urllib2.Request(url)
        #logger.debug("Waiting for response: %s", url)
        response = urllib2.urlopen(req)
        #logger.debug("Got reponse")
        page = response.read()
        result = json.loads(page)
        return result["response"]

    def ping(self, worker):
        self._request('/api/ping', {'worker': worker})  # Keep-alive

    def add_task(self, task, status, worker):
        self._request('/api/task', \
            {'worker': worker, 'task': task, 'status': status})

    def add_dep(self, task, task_2, worker):
        self._request('/api/dep', \
            {'worker': worker, 'task': task, 'dep_task': task_2})

    def get_work(self, worker):
        #time.sleep(1.0)
        done, task = self._request('/api/work', {'worker': worker})
        if done:
            return True, None
        else:
            return False, task

    def status(self, task, status, expl, worker):
        self._request('/api/status', \
            {'worker': worker, 'task': task, 'status': status, 'expl': expl})


class RemoteSchedulerResponder(object):
    """ Use on the server side for responding to requests"""

    def __init__(self, scheduler):
        self._scheduler = scheduler

    def task(self, task, worker, status='PENDING'):
        return self._scheduler.add_task(task, worker, status)

    def dep(self, task, dep_task, worker):
        return self._scheduler.add_dep(task, dep_task, worker)

    def work(self, worker):
        return self._scheduler.get_work(worker)

    def ping(self, worker):
        return self._scheduler.ping(worker)

    def status(self, task, status, worker, expl=None):
        return self._scheduler.status(task, status, worker, expl)

    def graph(self):
        return self._scheduler.graph()

    index = graph
