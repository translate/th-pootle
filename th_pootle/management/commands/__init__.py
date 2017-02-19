# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

from django import db
from django.core.management.base import BaseCommand
from django.utils.functional import cached_property

from th_pootle.utils import (
    CSVLoader, CSVMangler, MySqlInfile, MySqlOutfile, MySqlManager)


class MySqlCommand(BaseCommand):

    @cached_property
    def cursor(self):
        return db.connection.cursor()

    @cached_property
    def infile(self):
        return MySqlInfile(self.cursor)

    @cached_property
    def outfile(self):
        return MySqlOutfile(self.cursor)

    @cached_property
    def manager(self):
        return MySqlManager(self.cursor)

    @property
    def csv_loader(self):
        return CSVLoader

    @property
    def csv_mangler(self):
        return CSVMangler
