# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

import csv

from django.utils.functional import cached_property

from pootle.core.user import get_system_user_id
from pootle_statistics.models import SubmissionTypes
from pootle_store.models import Unit

from th_pootle.utils import FastMigration, MySqlCSVWriter, UnicodeCSVReader


class UnitSourceCreatedByMigration0033(FastMigration):

    @cached_property
    def sysuser(self):
        return get_system_user_id()

    @property
    def dump(self):
        return {
            "creation_submissions": dict(
                table="pootle_app_submission",
                columns=["unit_id", "submitter_id"],
                where=dict(
                    type=SubmissionTypes.UNIT_CREATE,
                    submitter_id__ne=self.sysuser)),
            "unit_pks": dict(
                columns=["id"],
                table="pootle_store_unit")}

    schema = {}
    data = {}
    create = {}
    load = {}
    alter = {}

    @cached_property
    def counter(self):
        class Counter(object):
            count = 0
        return Counter()

    def unit_mangler(self, unit):
        self.counter.count += 1
        return (
            str(self.counter.count),
            str(self.parsed["creation_submissions"].get(
                unit, self.sysuser)),
            unit)

    @property
    def mangle(self):
        reader_kwargs = dict(
            lineterminator='\n',
            escapechar="\\",
            doublequote=False)
        writer_kwargs = dict(
            lineterminator='\n',
            escapechar="\\",
            quotechar='',
            doublequote=False,
            quoting=csv.QUOTE_NONE)
        return {
            "units": dict(
                source="unit_pks",
                target="new_units",
                parse=["creation_submissions"],
                mangler=self.unit_mangler,
                reader_class=UnicodeCSVReader,
                reader_kwargs=reader_kwargs,
                writer_class=MySqlCSVWriter,
                writer_kwargs=writer_kwargs)}

    load = {
        "new_units": dict(
            table="pootle_store_unit_source",
            force=True)}


class UnitCreatedByMigration0027(FastMigration):
    data = {}
    dump = {
        "creation_submissions": dict(
            table="pootle_app_submission",
            columns=["unit_id", "submitter_id"],
            where=dict(
                type=SubmissionTypes.UNIT_CREATE,
                submitter_id__ne=get_system_user_id())),
        "unit_data": dict(
            table="pootle_store_unit")}
    schema = {
        "units": dict(
            table="pootle_store_unit",
            columns=["column_name"],
            optionally_enclose=None)}

    @property
    def data(self):
        return {
            "units": dict(
                schema=True,
                ordered=True)}

    @property
    def mangle(self):
        reader_kwargs = dict(
            lineterminator='\n',
            escapechar="\\",
            doublequote=False)
        writer_kwargs = dict(
            lineterminator='\n',
            escapechar="\\",
            quotechar='',
            doublequote=False,
            quoting=csv.QUOTE_NONE)
        return {
            "units": dict(
                source="unit_data",
                target="new_units",
                parse=["creation_submissions"],
                mangler=self.unit_mangler,
                reader_class=UnicodeCSVReader,
                reader_kwargs=reader_kwargs,
                writer_class=MySqlCSVWriter,
                writer_kwargs=writer_kwargs)}

    load = {
        "new_units": dict(
            table="pootle_store_unit",
            force=True)}

    @property
    def unitmeta(self):
        return self.parsed.get("schema__units")

    @cached_property
    def system_user_id(self):
        return get_system_user_id()

    @property
    def unitfields(self):
        return [
            "id", "unit_id", "created_by_id"]

    @cached_property
    def indices(self):
        keys = self.unitmeta.keys()
        return {
            k: keys.index(k)
            for k in keys
            if k in keys}

    @cached_property
    def string_indices(self):
        return [
            self.indices["source_f"],
            self.indices["target_f"]]

    @cached_property
    def maybe_string_indices(self):
        return [
            self.indices["translator_comment"],
            self.indices["developer_comment"],
            self.indices["context"],
            self.indices["locations"],
            self.indices["unitid_hash"],
            self.indices["source_hash"],
            self.indices["unitid"]]

    @cached_property
    def date_indices(self):
        return [
            self.indices["mtime"],
            self.indices["submitted_on"],
            self.indices["creation_time"],
            self.indices["reviewed_on"]]

    @cached_property
    def nullable(self):
        return [
            i for i
            in self.indices.values()
            if i not in (
                    self.string_indices
                    + [self.indices["unitid"]])]

    @cached_property
    def counter(self):
        class Counter(object):
            count = 0
        return Counter()

    validate = {
        "pootle_store_unit_temp": dict(
            model=Unit,
            columns=[
                "id", "source_f", "target_f",
                "developer_comment", "translator_comment"])}

    def unit_mangler(self, *unit):
        assert len(unit) + 1 == len(self.unitmeta)
        self.counter.count += 1
        submitter_id = self.parsed["creation_submissions"].get(
            unit[self.indices["id"]], self.system_user_id)
        _unit = []
        maybe_strings = (
            self.string_indices
            + self.date_indices
            + self.maybe_string_indices)
        for i, col in enumerate(unit):
            if col == "N" and i in self.nullable:
                _unit.append(u"__☠__")
            elif i in maybe_strings:
                col = (
                    u'"%s"'
                    % col.replace('\\', u'\\\\')
                    .replace('"', u'\\"')
                         .replace('\n', u"\\n"))
                _unit.append(col)
            else:
                _unit.append(col)
        _unit.append(str(submitter_id))
        return _unit
