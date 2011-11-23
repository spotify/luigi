import os
from spotify.builder3 import * 

class A(ExternalTask):
	def output(self):
		return File("/tmp/foo")

@expose
class B(Task):
	def requires(self):
		return A()
	
	def run(self):
		f = self.input().open('r')
		print f.read()
		f.close()
		self.input().remove()


if __name__ == "__main__":
	s = RemoteScheduler('foo')
	s.add(B())
	s.run()
