# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

import logging
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'pootle.settings'

from django import db
from django.core.management.base import BaseCommand, CommandError
from django.utils.functional import cached_property

from th_pootle.delegate import fast_migration

logger = logging.getLogger(__name__)

MESSAGES = dict(
    dump=(
        "Table %(table)s with %(rows)s rows "
        "dumped to '%(filepath)s' "
        "in %(timing)s seconds"),
    schema=(
        "Table %(table)s with %(rows)s rows "
        "dumped to '%(filepath)s' "
        "in %(timing)s seconds"),
    parse=(
        "Data '%(name)s' loaded from '%(source)s' "
        "with %(count)s/%(total)s rows "
        "in %(timing)s seconds"),
    mangle=(
        "Data '%(name)s' mangled from %(source)s to '%(target)s' "
        "with %(count)s/%(total)s rows "
        "in %(timing)s seconds"),
    load=(
        "Data loaded from %(filepath)s to table '%(table)s' "
        "with %(rows)s/%(rows)s rows "
        "in %(timing)s seconds"),
    validate=(
        "Validated '%(table)s' (%(columns)s) with %(model)s "
        "with %(count)s/%(total)s rows "
        "in %(timing)s seconds"),
    alter=(
        "Table '%(table)s' columns dropped (%(columns)s) "
        "in %(timing)s seconds"))


class Command(BaseCommand):
    requires_system_checks = False
    help = (
        "Run fast migrations using mysql's select into outfile, and "
        "load data infile syntax")

    def add_arguments(self, parser):
        parser.add_argument(
            'migration_name',
            action='store',
            choices=self.fast_migrations.keys())
        parser.add_argument(
            '-p', '--path',
            action='store',
            default='/tmp')
        parser.add_argument(
            '-a', '--action',
            action='store')
        parser.add_argument(
            "-n", '--name',
            action='store')

    @cached_property
    def fast_migrations(self):
        return fast_migration.gather()

    def get_migration(self, name):
        return self.fast_migrations[name]()

    def handle(self, **options):
        try:
            return self.handle_migrations(**options)
        except (db.IntegrityError, db.OperationalError) as e:
            raise CommandError(e)

    def handle_migrations(self, **options):
        actions = (
            [options["action"]]
            if options["action"]
            else None)
        path = options["path"]
        migration = self.get_migration(options["migration_name"])
        migrations = migration.migrate(
            actions=actions,
            name=options["name"],
            path=path)
        for action, result in migrations:
            self.stdout.write(MESSAGES[action] % result)
