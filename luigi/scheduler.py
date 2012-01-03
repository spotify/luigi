# TODO: refactor a lot. Have an interface that has the following methods:
#
# - add_dep(task1, task2)
# - add_task(task)
# - get_work()
# - ping()
# - status(task, status)
#
# Then add a small wrapper called RPCScheduler that just relays stuff
# to a scheduler that is not running in the same context.
#
# This way we can isolute the scheduling logic from the RPC code.

class LocalScheduler(object):
    def __init__(self):
        self.__scheduled = set()
        self.__schedule = []

    def add(self, task):
        if task.complete(): return
        if str(task) in self.__scheduled: return

        self.__scheduled.add(str(task))

        for task_2 in task.deps():
            self.add(task_2)

        self.__schedule.append(task)

    def run(self):
        print 'will run', self.__schedule
        for task in self.__schedule:
            # check inputs again
            for task_2 in task.deps():
                if not task_2.complete():
                    print task,'has dependency', task_2, 'which is not complete',
                    break
            else:
                task.run()

class RemoteScheduler(object):
    # TODO: move this class into its own file
    #       move the RPC part into rpc.py

    def __init__(self, client=None, host='localhost', port=8081):
        self.__scheduled = {}
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
                    scheduler.ping()

        k = KeepAliveThread()
        k.daemon = True
        k.start()
        

    def request(self, url, data):
        import urllib, urllib2, json
        # TODO(erikbern): do POST requests instead
        data = {'data': json.dumps(data)}
        url = 'http://%s:%d%s?%s' % (self.__host, self.__port, url, urllib.urlencode(data))
        # print url
        req = urllib2.Request(url)
        response = urllib2.urlopen(req)
        page = response.read()
        result = json.loads(page)
        return result

    def ping(self):
        self.request('/api/ping', {'client': self.__client}) # Keep-alive

    def add(self, task):
        s = str(task)
        if task.complete():
            self.request('/api/task', {'client': self.__client, 'task': s, 'status': 'DONE'})
            return False
        if s in self.__scheduled: return True
        self.__scheduled[s] = task

        if task.run != NotImplemented:
            self.request('/api/task', {'client': self.__client, 'task': s})

        for task_2 in task.deps():
            s_2 = str(task_2)
            self.add(task_2)
            self.request('/api/dep', {'client': self.__client, 'task': s, 'dep-task': s_2})

        return True # Will be done

    def run(self):
        while True:
            import time
            time.sleep(1.0)
            result = self.request('/api/work', {'client': self.__client})
            print result
            if result['done']: break
            s = result['task']
            if not s: continue
            s = str(s) # unicode -> str

            # TODO: we should verify that all dependencies exist (can't trust the server all the time)
            try:
                self.__scheduled[s].run()            
                status = 'DONE'
            except KeyboardInterrupt:
                raise
            except:
                import sys, traceback
                
                print sys.exc_info()[0], sys.exc_info()[1]
                print traceback.format_exc(sys.exc_info()[2]) # TODO: send traceback to server

                status = 'FAILED'
                
            self.request('/api/status', {'client': self.__client, 'task': s, 'status': status})
