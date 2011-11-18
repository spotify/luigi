import urllib
import urllib2
import json
from task import flatten

class LocalScheduler(object):
    def __init__(self):
        self.__scheduled = set()
        self.__schedule = []

    def add(self, task):
        if task.complete(): return
        if task in self.__scheduled: return

        self.__scheduled.add(task)

        for task_2 in flatten(task.requires()):
            self.add(task_2)

        self.__schedule.append(task)

    def run(self):
        print 'will run', self.__schedule
        for task in self.__schedule:
            # check inputs again
            for task_2 in flatten(task.requires()):
                if not task_2.complete():
                    print task,'has dependency', task_2, 'which is not complete', 
                    break
            else:
                task.run()

class RemoteScheduler(object):
    def __init__(self, client = "test"):
        self.__scheduled = {}
        self.__client = client

    def request(self, url, data):
        data = {'data': json.dumps(data)}
        req = urllib2.Request('http://localhost:8081' + url + '?' + urllib.urlencode(data))
        response = urllib2.urlopen(req)
        page = response.read()
        result = json.loads(page)
        return result

    def add(self, task):
        if task.complete(): return False
        s = str(task)
        if s in self.__scheduled: return True
        self.__scheduled[s] = task

        if task.run != NotImplemented:
            self.request('/api/product', {'client': self.__client, 'product': s})

        for task_2 in flatten(task.requires()):
            s_2 = str(task_2)
            if self.add(task_2):
                self.request('/api/dep', {'client': self.__client, 'product': s, 'dep-product': s_2})

        return True # Will be done

    def run(self):
        while True:
            import time
            time.sleep(1.0)
            result = self.request('/api/work', {'client': self.__client})
            print result
            s = result['product']
            if not s: continue
            s = str(s) # unicode -> str

            # TODO: we should verify that all dependencies exist (can't trust the server all the time)
            try:
                self.__scheduled[s].run()            
                status = 'OK'
            except KeyboardInterrupt:
                raise
            except:
                import sys, traceback
                
                print sys.exc_info()[0], sys.exc_info()[1]
                print traceback.format_exc(sys.exc_info()[2])

                status = 'FAILED'
                
            self.request('/api/status', {'client': self.__client, 'product': s, 'status': status})
