import urllib
import urllib2
import logging
import json
from scheduler import Scheduler, PENDING

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

    def add_task(self, worker, task_id, status=PENDING, runnable=False, deps=None, expl=None):
        self._request('/api/add_task', \
            {'task_id': task_id,
             'worker': worker,
             'status': status,
             'runnable': runnable,
             'deps': deps,
             'expl': expl,
             })

    def get_work(self, worker):
        return self._request('/api/get_work', {'worker': worker})


class RemoteSchedulerResponder(object):
    """ Use on the server side for responding to requests"""

    def __init__(self, scheduler):
        self._scheduler = scheduler

    def add_task(self, worker, task_id, status, runnable, deps, expl):
        return self._scheduler.add_task(worker, task_id, status, runnable, deps, expl)

    def get_work(self, worker):
        return self._scheduler.get_work(worker)

    def ping(self, worker):
        return self._scheduler.ping(worker)

    def graph(self):
        return self._scheduler.graph()

    index = graph
