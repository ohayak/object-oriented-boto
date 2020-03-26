#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=unused-argument
"""Test base objects."""
from edfred.oob.awslambda import Handler, Event
import mock


def test_base_event():
    """Test base Event."""
    event = Event()({}, {})
    event.payload = {'hi': 'hello'}
    event.context = {'hi': 'hello'}
    assert event.payload['hi'] == 'hello'
    assert event.context['hi'] == 'hello'


def test_base_handler():
    """Test base Handler."""


    class TestHandler(Handler):
        """Test handler."""

        def perform(self, event, **k):
            """Test perform method."""
            return event.payload['value']

    test_handler = TestHandler()
    event_object = {'value': 1.0}
    invocation = test_handler(event_object, {})

    assert invocation == 1.0