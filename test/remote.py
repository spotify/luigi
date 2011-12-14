
    def test_remote(self):
        port = 28913

        import threading
        def run_server():
            from spotify.luigi.server import Server
            s = Server(port=port)
            s.run()

        t = threading.Thread(target=run_server)
        t.start()

        import time
        time.sleep(1.0)

        luigi.run(['--port', str(port), 'Fib', '--n', '100']) # TODO: test command line separately

        self.assertEqual(MockFile._file_contents['/tmp/fib_10'], '55\n')
        self.assertEqual(MockFile._file_contents['/tmp/fib_100'], '354224848179261915075\n')

