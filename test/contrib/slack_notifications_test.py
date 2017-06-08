

from helpers import (unittest, with_config)
import mock
import json
import luigi
from luigi.contrib.slack_notifications import SlackNotifyHandling, slack, configure_slack


def mocked_requests(ret_code=200):

    class MockResponse:
        def __init__(self, body, status_code):
            self.body = body
            self.status_code = status_code

        @property
        def text(self):
            return self.body

        def raise_for_status(self):
            return None

    def mocked(*args, **kwargs):
        return MockResponse('', ret_code)

    return mocked


def mocked_event_handler(*args, **kwargs):
    def setup_handler(callback):
        return callback

    return setup_handler


class TestSlackTask(luigi.Task):

    @property
    def custom_property(self):
        return 'custom-value'


DEMO_WEBHOOK = 'http://some-webhook/'
BASIC_CONFIG = {'slack': {'webhook': DEMO_WEBHOOK}}


class SlackNotificationsTestCase(unittest.TestCase):

    @with_config({'slack': {}})
    def test_not_configured(self):
        self.slack_notify = SlackNotifyHandling(slack())
        self.assertFalse(self.slack_notify.is_configured)

    @with_config(BASIC_CONFIG)
    def test_is_configured(self):
        self.slack_notify = SlackNotifyHandling(slack())
        self.assertTrue(self.slack_notify.is_configured)

    @mock.patch('requests.post', side_effect=mocked_requests(200))
    @with_config(BASIC_CONFIG)
    def test_send_slack_message(self, mock_post):
        self.slack_notify = SlackNotifyHandling(slack())
        ret = self.slack_notify.send_slack_message('some-message')
        mock_post.assert_called_once_with('http://some-webhook/', data='{"text": "some-message"}')
        self.assertTrue(ret)

    @mock.patch('requests.post', side_effect=mocked_requests(400))
    @with_config(BASIC_CONFIG)
    def test_failed_send_slack_message(self, mock_post):
        self.slack_notify = SlackNotifyHandling(slack())
        ret = self.slack_notify.send_slack_message('some-message')
        mock_post.assert_called_once_with('http://some-webhook/', data='{"text": "some-message"}')
        self.assertFalse(ret)

    @mock.patch('requests.post', side_effect=mocked_requests())
    @with_config({'slack': {
        'webhook': DEMO_WEBHOOK,
        'channel': '#some-channel',
        'icon_emoji': ':some-icon-emoji:',
        'username': 'someusername'}})
    def test_send_configured_slack_message(self, mock_post):
        self.slack_notify = SlackNotifyHandling(slack())
        self.slack_notify.send_slack_message('some-message')
        mock_post.assert_called_once_with(DEMO_WEBHOOK, data=mock.ANY)
        args, kwargs = mock_post.call_args
        self.assertEquals(json.loads(kwargs['data']), {
            "icon_emoji": ":some-icon-emoji:",
            "text": "some-message",
            "channel": "#some-channel",
            "username": "someusername"})


class SlackNotificationsSetupEventHandlerTestCase(unittest.TestCase):

    def tearDown(self):
        SlackNotifyHandling._is_set_handlers = False

    @with_config(BASIC_CONFIG)
    @mock.patch('luigi.Task.event_handler', side_effect=mocked_event_handler)
    def test_set_default_handlers(self, mock_event_handler):
        self.slack_notify = SlackNotifyHandling(slack())
        self.slack_notify.set_handlers()
        calls = [mock.call(luigi.Event.START),
                 mock.call(luigi.Event.SUCCESS),
                 mock.call(luigi.Event.FAILURE)]
        mock_event_handler.assert_has_calls(calls, any_order=True)

    @with_config(BASIC_CONFIG)
    @mock.patch('luigi.Task.event_handler', side_effect=mocked_event_handler)
    def test_set_handlers_only_once(self, mock_event_handler):
        self.slack_notify = SlackNotifyHandling(slack())
        self.slack_notify.set_handlers()
        mock_event_handler.reset_mock()
        self.slack_notify.set_handlers()
        self.assertFalse(mock_event_handler.called)

    @with_config({'slack': {'webhook': DEMO_WEBHOOK, 'events': '["START", "custom.event"]'}})
    @mock.patch('luigi.Task.event_handler', side_effect=mocked_event_handler)
    def test_custom_handlers(self, mock_event_handler):
        self.slack_notify = SlackNotifyHandling(slack())
        self.slack_notify.set_handlers()
        calls = [mock.call(luigi.Event.START),
                 mock.call('custom.event')]
        mock_event_handler.assert_has_calls(calls, any_order=True)

    @with_config(BASIC_CONFIG)
    @mock.patch('luigi.Task.event_handler', side_effect=mocked_event_handler)
    def test_configure_handlers_slack(self, mock_event_handler):
        configure_slack()
        calls = [mock.call(luigi.Event.START),
                 mock.call(luigi.Event.SUCCESS),
                 mock.call(luigi.Event.FAILURE)]
        mock_event_handler.assert_has_calls(calls, any_order=True)


class SlackNotificationsTriggerEventTestCase(unittest.TestCase):

    def setUp(self):
        self.demo_task = TestSlackTask()

    def tearDown(self):
        SlackNotifyHandling._is_set_handlers = False
        luigi.Task._event_callbacks = {}

    @with_config(BASIC_CONFIG)
    def test_send_message_on_trigger_start(self):
        self.slack_notify = SlackNotifyHandling(slack())
        mocked = self.slack_notify.send_slack_message = mock.Mock()
        self.slack_notify.set_handlers()
        self.demo_task.trigger_event(luigi.Event.START, self.demo_task)
        mocked.assert_called_once_with('Luigi task TestSlackTask__99914b932b event START: () {}')

    @with_config({'slack': {
        'webhook': DEMO_WEBHOOK,
        'template': 'Custom template {task_id} event {event} - {task.custom_property}'}})
    def test_send_message_with_custom_template(self):
        self.slack_notify = SlackNotifyHandling(slack())
        mocked = self.slack_notify.send_slack_message = mock.Mock()
        self.slack_notify.set_handlers()
        self.demo_task.trigger_event(luigi.Event.SUCCESS, self.demo_task)
        mocked.assert_called_once_with('Custom template TestSlackTask__99914b932b event SUCCESS - custom-value')

    @with_config({'slack': {
        'webhook': DEMO_WEBHOOK,
        'event_templates': '{"START": "custom template for start {task_id}"}'}})
    def test_send_message_with_custom_event_template(self):
        self.slack_notify = SlackNotifyHandling(slack())
        mocked = self.slack_notify.send_slack_message = mock.Mock()
        self.slack_notify.set_handlers()
        self.demo_task.trigger_event(luigi.Event.START, self.demo_task)
        self.demo_task.trigger_event(luigi.Event.SUCCESS, self.demo_task)
        calls = [mock.call('custom template for start TestSlackTask__99914b932b'),
                 mock.call('Luigi task TestSlackTask__99914b932b event SUCCESS: () {}')]
        mocked.assert_has_calls(calls)
