"""
Test the visualiser's javascript using PhantomJS.

"""

import os
import luigi
import subprocess
import sys
import unittest
import time
import threading

from selenium import webdriver

here = os.path.dirname(__file__)

# Patch-up path so that we can import from the directory above this one.r
# This seems to be necessary because the `test` directory has no __init__.py but
# adding one makes other tests fail.
sys.path.append(os.path.join(here, '..'))
from server_test import ServerTestBase  # noqa

TEST_TIMEOUT = 40


@unittest.skipUnless(os.environ.get('TEST_VISUALISER'),
                     'PhantomJS tests not requested in TEST_VISUALISER')
class TestVisualiser(ServerTestBase):
    """
    Builds a medium-sized task tree of MergeSort results then starts
    phantomjs  as a subprocess to interact with the scheduler.

    """

    def setUp(self):
        super(TestVisualiser, self).setUp()

        x = 'I scream for ice cream'
        task = UberTask(base_task=FailingMergeSort, x=x, copies=4)
        luigi.build([task], workers=1, scheduler_port=self.get_http_port())

        self.done = threading.Event()

        def _do_ioloop():
            # Enter ioloop for maximum TEST_TIMEOUT.  Check every 2s whether the test has finished.
            print('Entering event loop in separate thread')

            for i in range(TEST_TIMEOUT):
                try:
                    self.wait(timeout=1)
                except AssertionError:
                    pass
                if self.done.is_set():
                    break

            print('Exiting event loop thread')

        self.iothread = threading.Thread(target=_do_ioloop)
        self.iothread.start()

    def tearDown(self):
        self.done.set()
        self.iothread.join()

    def test(self):
        port = self.get_http_port()
        print('Server port is {}'.format(port))
        print('Starting phantomjs')

        p = subprocess.Popen('phantomjs {}/phantomjs_test.js http://localhost:{}'.format(here, port),
                             shell=True, stdin=None)

        # PhantomJS may hang on an error so poll
        status = None
        for x in range(TEST_TIMEOUT):
            status = p.poll()
            if status is not None:
                break
            time.sleep(1)

        if status is None:
            raise AssertionError('PhantomJS failed to complete')
        else:
            print('PhantomJS return status is {}'.format(status))
            assert status == 0

    # tasks tab tests.
    def test_keeps_entries_after_page_refresh(self):
        port = self.get_http_port()
        driver = webdriver.PhantomJS()

        driver.get('http://localhost:{}'.format(port))

        length_select = driver.find_element_by_css_selector('select[name="taskTable_length"]')
        assert length_select.get_attribute('value') == '10'
        assert len(driver.find_elements_by_css_selector('#taskTable tbody tr')) == 10

        # Now change entries select box and check again.
        clicked = False
        for option in length_select.find_elements_by_css_selector('option'):
            if option.text == '50':
                option.click()
                clicked = True
                break

        assert clicked, 'Could not click option with "50" entries.'

        assert length_select.get_attribute('value') == '50'
        assert len(driver.find_elements_by_css_selector('#taskTable tbody tr')) == 50

        # Now refresh page and check. Select box should be 50 and table should contain 50 rows.
        driver.refresh()

        # Once page refreshed we have to find all selectors again.
        length_select = driver.find_element_by_css_selector('select[name="taskTable_length"]')
        assert length_select.get_attribute('value') == '50'
        assert len(driver.find_elements_by_css_selector('#taskTable tbody tr')) == 50

    def test_keeps_table_filter_after_page_refresh(self):
        port = self.get_http_port()
        driver = webdriver.PhantomJS()

        driver.get('http://localhost:{}'.format(port))

        # Check initial state.
        search_input = driver.find_element_by_css_selector('input[type="search"]')
        assert search_input.get_attribute('value') == ''
        assert len(driver.find_elements_by_css_selector('#taskTable tbody tr')) == 10

        # Now filter and check filtered table.
        search_input.send_keys('ber')
        # UberTask only should be displayed.
        assert len(driver.find_elements_by_css_selector('#taskTable tbody tr')) == 1

        # Now refresh page and check. Filter input should contain 'ber' and table should contain
        # one row (UberTask).
        driver.refresh()

        # Once page refreshed we have to find all selectors again.
        search_input = driver.find_element_by_css_selector('input[type="search"]')
        assert search_input.get_attribute('value') == 'ber'
        assert len(driver.find_elements_by_css_selector('#taskTable tbody tr')) == 1

    def test_keeps_order_after_page_refresh(self):
        port = self.get_http_port()
        driver = webdriver.PhantomJS()

        driver.get('http://localhost:{}'.format(port))

        # Order by name (asc).
        column = driver.find_elements_by_css_selector('#taskTable thead th')[1]
        column.click()

        table_body = driver.find_element_by_css_selector('#taskTable tbody')
        assert self._get_cell_value(table_body, 0, 1) == 'FailingMergeSort_0'

        # Ordery by name (desc).
        column.click()
        assert self._get_cell_value(table_body, 0, 1) == 'UberTask'

        # Now refresh page and check. Table should be ordered by name (desc).
        driver.refresh()

        # Once page refreshed we have to find all selectors again.
        table_body = driver.find_element_by_css_selector('#taskTable tbody')
        assert self._get_cell_value(table_body, 0, 1) == 'UberTask'

    def test_keeps_filter_on_server_after_page_refresh(self):
        port = self.get_http_port()
        driver = webdriver.PhantomJS()

        driver.get('http://localhost:{}/static/visualiser/index.html#tab=tasks'.format(port))

        # Check initial state.
        checkbox = driver.find_element_by_css_selector('#serverSideCheckbox')
        assert checkbox.is_selected() is False

        # Change invert checkbox.
        checkbox.click()

        # Now refresh page and check. Invert checkbox shoud be checked.
        driver.refresh()

        # Once page refreshed we have to find all selectors again.
        checkbox = driver.find_element_by_css_selector('#serverSideCheckbox')
        assert checkbox.is_selected()

    def test_synchronizes_fields_on_tasks_tab(self):
        # Check fields population if tasks tab was opened by direct link
        port = self.get_http_port()
        driver = webdriver.PhantomJS()
        url = 'http://localhost:{}/static/visualiser/index.html' \
              '#tab=tasks&length=50&search__search=er&filterOnServer=1&order=1,desc' \
              .format(port)

        driver.get(url)

        length_select = driver.find_element_by_css_selector('select[name="taskTable_length"]')
        assert length_select.get_attribute('value') == '50'

        search_input = driver.find_element_by_css_selector('input[type="search"]')
        assert search_input.get_attribute('value') == 'er'
        assert len(driver.find_elements_by_css_selector('#taskTable tbody tr')) == 50

        # Table is ordered by first column (name)
        table_body = driver.find_element_by_css_selector('#taskTable tbody')
        assert self._get_cell_value(table_body, 0, 1) == 'UberTask'

    # graph tab tests.

    def test_keeps_invert_after_page_refresh(self):
        port = self.get_http_port()
        driver = webdriver.PhantomJS()

        driver.get('http://localhost:{}/static/visualiser/index.html#tab=graph'.format(port))

        # Check initial state.
        invert_checkbox = driver.find_element_by_css_selector('#invertCheckbox')
        assert invert_checkbox.is_selected() is False

        # Change invert checkbox.
        invert_checkbox.click()

        # Now refresh page and check. Invert checkbox shoud be checked.
        driver.refresh()

        # Once page refreshed we have to find all selectors again.
        invert_checkbox = driver.find_element_by_css_selector('#invertCheckbox')
        assert invert_checkbox.is_selected()

    def test_keeps_task_id_after_page_refresh(self):
        port = self.get_http_port()
        driver = webdriver.PhantomJS()

        driver.get('http://localhost:{}/static/visualiser/index.html#tab=graph'.format(port))

        # Check initial state.
        task_id_input = driver.find_element_by_css_selector('#js-task-id')
        assert task_id_input.get_attribute('value') == ''

        # Change task id
        task_id_input.send_keys('1')
        driver.find_element_by_css_selector('#loadTaskForm button[type=submit]').click()

        # Now refresh page and check. Task ID field should contain 1
        driver.refresh()

        # Once page refreshed we have to find all selectors again.
        task_id_input = driver.find_element_by_css_selector('#js-task-id')
        assert task_id_input.get_attribute('value') == '1'

    def test_keeps_hide_done_after_page_refresh(self):
        port = self.get_http_port()
        driver = webdriver.PhantomJS()

        driver.get('http://localhost:{}/static/visualiser/index.html#tab=graph'.format(port))

        # Check initial state.
        hide_done_checkbox = driver.find_element_by_css_selector('#hideDoneCheckbox')
        assert hide_done_checkbox.is_selected() is False

        # Change invert checkbox.
        hide_done_checkbox.click()

        # Now refresh page and check. Invert checkbox shoud be checked.
        driver.refresh()

        # Once page refreshed we have to find all selectors again.
        hide_done_checkbox = driver.find_element_by_css_selector('#hideDoneCheckbox')
        assert hide_done_checkbox.is_selected()

    def test_keeps_visualisation_type_after_page_refresh(self):
        port = self.get_http_port()
        driver = webdriver.PhantomJS()

        driver.get('http://localhost:{}/static/visualiser/index.html#tab=graph'.format(port))

        # Check initial state.
        svg_radio = driver.find_element_by_css_selector('input[value=svg]')
        assert svg_radio.is_selected()

        # Change vistype to d3 by clicking on its label.
        d3_radio = driver.find_element_by_css_selector('input[value=d3]')
        d3_radio.find_element_by_xpath('..').click()

        # Now refresh page and check. D3 checkbox shoud be checked.
        driver.refresh()

        # Once page refreshed we have to find all selectors again.
        d3_radio = driver.find_element_by_css_selector('input[value=d3]')
        assert d3_radio.is_selected()

    def test_synchronizes_fields_on_graph_tab(self):
        # Check fields population if tasks tab was opened by direct link.
        port = self.get_http_port()
        driver = webdriver.PhantomJS()
        url = 'http://localhost:{}/static/visualiser/index.html' \
              '#tab=graph&taskId=1&invert=1&hideDone=1&visType=svg' \
              .format(port)
        driver.get(url)

        # Check task id input
        task_id_input = driver.find_element_by_css_selector('#js-task-id')
        assert task_id_input.get_attribute('value') == '1'

        # Check Show Upstream Dependencies checkbox.
        invert_checkbox = driver.find_element_by_css_selector('#invertCheckbox')
        assert invert_checkbox.is_selected()

        # Check Hide Done checkbox.
        hide_done_checkbox = driver.find_element_by_css_selector('#hideDoneCheckbox')
        assert hide_done_checkbox.is_selected()

        svg_radio = driver.find_element_by_css_selector('input[value=svg]')
        assert svg_radio.get_attribute('checked')

    def _get_cell_value(self, elem, row, column):
        tr = elem.find_elements_by_css_selector('#taskTable tbody tr')[row]
        td = tr.find_elements_by_css_selector('td')[column]
        return td.text


# ---------------------------------------------------------------------------
# Code for generating a tree of tasks with some failures.

def generate_task_families(task_class, n):
    """
    Generate n copies of a task with different task_family names.

    :param task_class: a subclass of `luigi.Task`
    :param n: number of copies of `task_class` to create
    :return: Dictionary of task_family => task_class

    """
    ret = {}
    for i in range(n):
        class_name = '{}_{}'.format(task_class.task_family, i)
        ret[class_name] = type(class_name, (task_class,), {})

    return ret


class UberTask(luigi.Task):
    """
    A task which depends on n copies of a configurable subclass.

    """
    _done = False

    base_task = luigi.TaskParameter()
    x = luigi.Parameter()
    copies = luigi.IntParameter()

    def requires(self):
        task_families = generate_task_families(self.base_task, self.copies)
        for class_name in task_families:
            yield task_families[class_name](x=self.x)

    def complete(self):
        return self._done

    def run(self):
        self._done = True


def popmin(a, b):
    """
    popmin(a, b) -> (i, a', b')

    where i is min(a[0], b[0]) and a'/b' are the results of removing i from the
    relevant sequence.
    """
    if len(a) == 0:
        return b[0], a, b[1:]
    elif len(b) == 0:
        return a[0], a[1:], b
    elif a[0] > b[0]:
        return b[0], a, b[1:]
    else:
        return a[0], a[1:], b


class MemoryTarget(luigi.Target):
    def __init__(self):
        self.box = None

    def exists(self):
        return self.box is not None


class MergeSort(luigi.Task):
    x = luigi.Parameter(description='A string to be sorted')

    def __init__(self, *args, **kwargs):
        super(MergeSort, self).__init__(*args, **kwargs)

        self.result = MemoryTarget()

    def requires(self):
        # Allows us to override behaviour in subclasses
        cls = self.__class__

        if len(self.x) > 1:
            p = len(self.x) // 2

            return [cls(self.x[:p]), cls(self.x[p:])]

    def output(self):
        return self.result

    def run(self):
        if len(self.x) > 1:
            list_1, list_2 = (x.box for x in self.input())

            s = []
            while list_1 or list_2:
                item, list_1, list_2 = popmin(list_1, list_2)
                s.append(item)
        else:
            s = self.x

        self.result.box = ''.join(s)


class FailingMergeSort(MergeSort):
    """
    Simply fail if the string to sort starts with ' '.

    """
    fail_probability = luigi.FloatParameter(default=0.)

    def run(self):
        if self.x[0] == ' ':
            raise Exception('I failed')
        else:
            return super(FailingMergeSort, self).run()


if __name__ == '__main__':
    x = 'I scream for ice cream'
    task = UberTask(base_task=FailingMergeSort, x=x, copies=4)
    luigi.build([task], workers=1, scheduler_port=8082)
