# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

import importlib

from django.apps import AppConfig


class THPootleConfig(AppConfig):

    name = "th_pootle"
    verbose_name = "Translate House Pootle extensions and plugins"

    def ready(self):
        importlib.import_module("th_pootle.models")
