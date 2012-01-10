class Scheduler(object):
    ''' Abstract base class

    Note that the methods all take string arguments, not Task objects...
    '''

    add_task = NotImplemented
    add_dep = NotImplemented
    get_work = NotImplemented
    ping = NotImplemented # TODO: remove?
    status = NotImplemented # merge with add_task?

class DummyScheduler(object):
    ''' DEPRECATED

    For local scheduling, now we are using CentralPlanner but with no RPC in between.
    However, this class is left as an example of a super small Schedule implementation
    that actually works fairly well.
    '''
    def __init__(self):
        import collections
        self.__schedule = []

    def add_task(self, task, status, worker):
        if status == 'PENDING':
            self.__schedule.append(task)

    def add_dep(self, task, status, worker):
        pass

    def get_work(self, worker):
        if len(self.__schedule):
            # TODO: check for dependencies:
            #for task_2 in task.deps():
            #    if not task_2.complete():
            #        print task,'has dependency', task_2, 'which is not complete',
            #        break
            return False, self.__schedule.pop()
        else:
            return True, None

    def status(self, task, status, expl, worker):
        pass

class RemoteScheduler(Scheduler):
    ''' Scheduler that just relays everything to a central planner

        TODO: Move this to rpc.py?
    '''

    def __init__(self, host='localhost', port=8081):
        self.__host = host
        self.__port = port

    def request(self, url, data):
        import urllib, urllib2, json
        # TODO(erikbern): do POST requests instead
        data = {'data': json.dumps(data)}
        url = 'http://%s:%d%s?%s' % (self.__host, self.__port, url, urllib.urlencode(data))
        req = urllib2.Request(url)
        response = urllib2.urlopen(req)
        page = response.read()
        result = json.loads(page)
        return result

    def ping(self, worker):
        self.request('/api/ping', {'worker': worker}) # Keep-alive

    def add_task(self, task, status, worker):
        self.request('/api/task', {'worker': worker, 'task': task, 'status': status})        

    def add_dep(self, task, task_2, worker):
        self.request('/api/dep', {'worker': worker, 'task': task, 'dep_task': task_2})

    def get_work(self, worker):
        import time
        time.sleep(1.0)
        done, task = self.request('/api/work', {'worker': worker})
        if done:
            return True, None
        else:
            return False, task

    def status(self, task, status, expl, worker):
        self.request('/api/status', {'worker': worker, 'task': task, 'status': status, 'expl': expl})
