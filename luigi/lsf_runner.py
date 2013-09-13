import os
import cPickle as pickle
import logging

def do_work_on_compute_node(work_dir):
    with open(os.path.join(work_dir, "job-instance.pickle"), "r") as f:
        job = pickle.load(f)
    job.work()

def main(args=sys.argv):
    """Run the work() method from the class instance in the file "job-instance.pickle".
    """
    try:
        # Set up logging.
        logging.basicConfig(level=logging.WARN)
        work_dir = args[1]
        assert os.path.exists(work_dir), "First argument to lsf_runner.py must be a directory that exists"
        do_work_on_compute_node(work_dir)
    except Exception, exc:
        # Dump encoded data that we will try to fetch using mechanize
        print(exc)
        raise

if __name__ == '__main__':
    main()
