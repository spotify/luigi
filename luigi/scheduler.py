class Scheduler(object):
    add_task = NotImplemented
    add_dep = NotImplemented
    get_work = NotImplemented
    ping = NotImplemented # TODO: remove?
    status = NotImplemented # merge with add_task?

class LocalScheduler(object):
    def __init__(self):
        import collections
        self.__schedule = collections.deque()

    def add_task(self, task, status):
        if status == 'PENDING':
            self.__schedule.append(task)

    def add_dep(self, task, status):
        pass

    def get_work(self):
        if len(self.__schedule):
            # TODO: check for dependencies:
            #for task_2 in task.deps():
            #    if not task_2.complete():
            #        print task,'has dependency', task_2, 'which is not complete',
            #        break
            return False, self.__schedule.popleft()
        else:
            return True, None

    def status(self, task, status, expl=None):
        pass

class RemoteScheduler(Scheduler):
    ''' Scheduler that just relays everything to a central planner

        TODO: Move this to rpc.py?
    '''

    def __init__(self, client=None, host='localhost', port=8081):
        self.__tasks = {}
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
        s = str(task)
        self.request('/api/task', {'client': self.__client, 'task': s, 'status': status})        
        self.__tasks[s] = task

    def add_dep(self, task, task_2):
        s = str(task)
        s_2 = str(task_2)
        self.request('/api/dep', {'client': self.__client, 'task': s, 'dep-task': s_2})

    def get_work(self):
        import time
        time.sleep(1.0)
        result = self.request('/api/work', {'client': self.__client})
        if result['done']:
            return True, None
        else:
            s = result['task']
            s = str(s) # unicode -> str
            task = self.__tasks[s]
            return False, task

class Worker(object):
    """ Simple class that talks to a scheduler and:
        - Tells the scheduler what it has to do
        - Asks for stuff to do
    """
    def __init__(self, scheduler):
        self.__scheduler = scheduler
        self.__scheduled_tasks = set()
    
    def add(self, task):
        if task in self.__scheduled_tasks: return
        self.__scheduled_tasks.add(task)

        if task.complete():
            self.__scheduler.add_task(task, status='DONE')
            return

        for task_2 in task.deps():
            self.add(task_2) # Schedule it recursively
            self.__scheduler.add_dep(task, task_2)

        self.__scheduler.add_task(task, status='PENDING')

    def run(self):
        while True:
            done, task = self.__scheduler.get_work()
            if done: break

            # TODO: we should verify that all dependencies exist (can't trust the scheduler all the time)
            try:
                task.run()            
                status, expl = 'DONE', None
            except KeyboardInterrupt:
                raise
            except:
                import sys, traceback
                
                status = 'FAILED'
                d = [sys.exc_info()[0], sys.exc_info()[1], traceback.format_exc(sys.exc_info()[2])]
                expl = '\n'.join(map(str, d))
                print expl
                
            self.__scheduler.status(task, status=status, expl=expl)

