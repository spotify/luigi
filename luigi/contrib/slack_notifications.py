# -*- coding: utf-8 -*-
#
# Copyright 2017 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

''' Supports sending slack notifications for different task events.

Usage:

Create an incomming webhook on slack: https://api.slack.com/incoming-webhooks

Setup the slack integration with the following code.
This can be done in the same module where the luigi tasks are defined.

.. code-block:: python

    from luigi.contrib.slack_notifications import configure_slack
    configure_slack()

Use a configuration like the following on luigi.cfg

.. code-block:: ini

    [slack]
    webhook = https://hooks.slack.com/services/something
    events = ["START", "SUCCESS", "FAILURE"]
    template = Luigi task {task_id} event {event}: {args} {kwargs}


Alternatives:

Another plugin with similar purpouse is luigi-slack (https://github.com/bonzanini/luigi-slack). The major differences between this and luigi-slack is that:

- Inspired by email notifications (notifications.py): simple configuration though luigi.cfg (no need for python configuration)
- Minimal changes to python code (only call configure_slack()), no need create a new wrapper around luigi.run.
- This only depends on requests module (does not depend on slackclient).
- Work both for python 2 and 3
- Configurable template, per event, that can expose specific task properties

'''


import logging
import luigi
import json

logger = logging.getLogger(__name__)

try:
    import requests
except ImportError:
    logger.warning("Module %s requires the python package 'requests'." % __name__)


class slack(luigi.Config):
    webhook = luigi.parameter.Parameter(
        default='',
        description='Webhook URL to send the notifications')
    channel = luigi.parameter.Parameter(
        default='',
        description='Channel to send slack notifications to')
    username = luigi.parameter.Parameter(
        default='',
        description='Slack username originating the message')
    icon_emoji = luigi.parameter.Parameter(
        default='',
        description='Slack bot icon emoji')
    events = luigi.parameter.ListParameter(
        default=['START', 'FAILURE', 'SUCCESS'],
        description='List of events to be handled as slack notifications')
    template = luigi.parameter.Parameter(
        default='Luigi task {task_id} event {event}: {args} {kwargs}',
        description='Template message for event handling slack notifications')
    event_templates = luigi.parameter.DictParameter(
        default={},
        description='Custom template message per event')


class SafeGetAttr(object):
    """
    Wraps an object returning a default_value for undefined attributes
    """
    def __init__(self, obj, default_value=None):
        self._obj = obj
        self._default_value = default_value

    def __getattr__(self, attr):
        return getattr(getattr(self, '_obj'), attr, getattr(self, '_default_value'))


class SlackNotifyHandling():
    slack_hook_url = None

    def __init__(self, slack_config):
        self.slack_hook_url = slack_config.webhook
        self.channel = slack_config.channel
        self.events = slack_config.events
        self.template = slack_config.template
        self.username = slack_config.username
        self.icon_emoji = slack_config.icon_emoji
        self.event_templates = slack_config.event_templates

    @property
    def is_configured(self):
        """
        Check if the slack web hook is configured so that slack messages can be sent
        """
        return bool(self.slack_hook_url)

    def _make_data(self, text):
        data = dict(text=text)
        if self.channel:
            data['channel'] = self.channel
        if self.username:
            data['username'] = self.username
        if self.icon_emoji:
            data['icon_emoji'] = self.icon_emoji
        return json.dumps(data)

    def send_slack_message(self, text):
        """
        Send a string as a slack message
        """
        r = requests.post(self.slack_hook_url, data=self._make_data(text))
        if not r.status_code == 200:
            logger.error('Could not notify slack, got error: %s ' % r.text)
            return False
        return True

    def _handler_for_template(self, template, event):
        def handle(task, *args, **kwargs):
            msg = template.format(
                event=event,
                args=args,
                kwargs=kwargs,
                task_id=task.task_id,
                task=SafeGetAttr(task, '')
            )
            self.send_slack_message(msg)
        return handle

    _is_set_handlers = False

    def set_handlers(self):
        """
        Configure all luigi.task.event_handler if slack notifications ar e configured
        To avoid setting up event handler multiple times, this only runs once
        """
        if not self.is_configured or self.__class__._is_set_handlers:
            return
        logger.info('Configuring slack notifications event handling...')
        event_aliases = {
            'START': luigi.Event.START,
            'SUCCESS': luigi.Event.SUCCESS,
            'FAILURE': luigi.Event.FAILURE
        }
        for event in self.events:
            luigi_event = event_aliases.get(event, event)
            template = self.event_templates.get(event, self.template)
            luigi.Task.event_handler(luigi_event)(self._handler_for_template(template, event))
        self.__class__._is_set_handlers = True


def configure_slack():
    """
    Setup task event handlers to send slack notifications
    """
    SlackNotifyHandling(slack()).set_handlers()
