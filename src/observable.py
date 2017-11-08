#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Observable(object):

    event_handlers = {
        'Event1': [],
        'Event2': [],
    }

    def subscribe(self, event_handlers):
        for event_name in event_handlers:
            if event_name in self.event_handlers:
                if callable(event_handlers[event_name]):
                    self.event_handlers[event_name].append(event_handlers[event_name])
                else:
                    if isinstance(event_handlers[event_name], list):
                        for callback in event_handlers[event_name]:
                            if callable(callback):
                                self.event_handlers[event_name].append(callback)

    def notify(self, event_name, **attrs):
        for callback in self.event_handlers[event_name]:
            callback(**attrs)

    def subscribers(self, event_name):
        return event_name in self.event_handlers and (len(self.event_handlers[event_name]) > 0)