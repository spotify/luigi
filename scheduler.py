from rule import flatten

class LocalScheduler(object):
    def __init__(self):
        self.__scheduled = set()
        self.__schedule = []

    def add(self, rule):
        if rule.exists(): return
        if rule in self.__scheduled: return

        self.__scheduled.add(rule)

        for rule_2 in flatten(rule.requires()):
            self.add(rule_2)

        self.__schedule.append(rule)

    def run(self):
        print 'will run', self.__schedule
        for rule in self.__schedule:
            # check inputs again
            for rule_2 in flatten(rule.requires()):
                if not rule_2.exists():
                    print 'dependency', rule_2, 'does not exist for', rule
                    break
            else:
                rule.run()

class RemoteScheduler(object):
    def __init__(self):
        self.__scheduled = {}
        self.__client = 'test'

    def request(self, url, data):
        import urllib2, json, urllib
        data = {'data': json.dumps(data)}
        req = urllib2.Request('http://localhost:8080' + url + '?' + urllib.urlencode(data))
        response = urllib2.urlopen(req)
        page = response.read()
        result = json.loads(page)
        return result

    def add(self, rule):
        if rule.exists(): return False
        s = str(rule)
        if s in self.__scheduled: return True
        self.__scheduled[s] = rule

        self.request('/api/product', {'client': self.__client, 'product': s, 'can-build': True})

        for rule_2 in flatten(rule.requires()):
            s_2 = str(rule_2)
            if self.add(rule_2):
                self.request('/api/dep', {'client': self.__client, 'product': s, 'dep-product': s_2})

        return True # Will be done

    def run(self):
        while True:
            import time
            time.sleep(1.0)
            result = self.request('/api/work', {'client': self.__client})
            s = result['product']
            if not s: continue

            self.__scheduled[s].run()            
            self.request('/api/status', {'client': self.__client, 'product': s, 'status': 'OK'})
