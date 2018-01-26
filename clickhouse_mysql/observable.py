#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Observable(object):
    """
    Implements Observable pattern
    """

    # functions to be called when event to be notified upon
    event_handlers = {
        'Event1': [],
        'Event2': [],
    }

    def subscribe(self, event_handlers):
        # event_handlers has the same structure as self.event_handlers

        for event_name in event_handlers:
            if event_name in self.event_handlers:
                # this event is listed in Observable as 'subscribable'
                if callable(event_handlers[event_name]):
                    # function itself
                    self.event_handlers[event_name].append(event_handlers[event_name])
                else:
                    # assume ist of functions - iterate over it and add each of them
                    if isinstance(event_handlers[event_name], list):
                        for callback in event_handlers[event_name]:
                            if callable(callback):
                                self.event_handlers[event_name].append(callback)

    def notify(self, event_name, **attrs):
        # notify (call function) each of subscribers of event_name event
        for callback in self.event_handlers[event_name]:
            callback(**attrs)

    def subscribers(self, event_name):
        # are there any (>0) subscribers for event event_name?
        return event_name in self.event_handlers and (len(self.event_handlers[event_name]) > 0)
