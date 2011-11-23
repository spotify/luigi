from spotify.builder3 import * 

class A(Task):
	def output(self):
		return File("/tmp/foo")
	
	def run(self):
		f = self.output().open('w')
		f.write("Tjena Tjena!\n")
		f.close()


if __name__ == "__main__":
	s = RemoteScheduler('bar')
	s.add(A())
	s.run()
