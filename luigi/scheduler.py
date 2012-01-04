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

    TODO: use CentralPlanner but with no RPC in between!
    '''
    def __init__(self):
        import collections
        self.__schedule = []

    def add_task(self, task, status, client=None):
        if status == 'PENDING':
            self.__schedule.append(task)

    def add_dep(self, task, status, client=None):
        pass

    def get_work(self, client=None):
        if len(self.__schedule):
            # TODO: check for dependencies:
            #for task_2 in task.deps():
            #    if not task_2.complete():
            #        print task,'has dependency', task_2, 'which is not complete',
            #        break
            return False, self.__schedule.pop()
        else:
            return True, None

    def status(self, task, status, expl=None, client=None):
        pass

class RemoteScheduler(Scheduler):
    ''' Scheduler that just relays everything to a central planner

        TODO: Move this to rpc.py?
    '''

    def __init__(self, client=None, host='localhost', port=8081):
        import random
        if not client: client = 'client-%09d' % random.randrange(0, 999999999)
        self.__client = client
        self.__host = host
        self.__port = port

        import threading, time
        scheduler = self

        class KeepAliveThread(threading.Thread):
            def run(self):
                while True:
                    time.sleep(1.0)
                    try:
                        scheduler.ping()
                    except httplib.BadStatusLine:
                        print 'WARNING: could not ping!'

        k = KeepAliveThread()
        k.daemon = True
        k.start()
        

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

    def ping(self):
        self.request('/api/ping', {'client': self.__client}) # Keep-alive

    def add_task(self, task, status):
        self.request('/api/task', {'client': self.__client, 'task': task, 'status': status})        

    def add_dep(self, task, task_2):
        self.request('/api/dep', {'client': self.__client, 'task': task, 'dep_task': task_2})

    def get_work(self):
        import time
        time.sleep(1.0)
        done, task = self.request('/api/work', {'client': self.__client})
        if done:
            return True, None
        else:
            return False, task

    def status(self, task, status, expl):        
        self.request('/api/status', {'client': self.__client, 'task': task, 'status': status, 'expl': expl})
