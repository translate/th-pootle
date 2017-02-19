# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

import csv
from collections import OrderedDict

from django.utils.functional import cached_property

# from pootle_statistics.models import Submission

from th_pootle.utils import FastMigration, MySqlCSVWriter, UnicodeCSVReader


class StatisticsMigration0011(FastMigration):
    dump = {
        "subs_data": dict(
            table="pootle_app_submission",
            where=dict(unit_id__gt=0, type__ne=10))}
    schema = {}
    mangle = {}
    data = {}
    create = {}
    alter = {}
    load = {
        "subs_data": dict(
            table="pootle_app_submission",
            force=True)}


class StatisticsMigration0008(FastMigration):
    dump = {
        "unit_revisions": dict(
            table="pootle_store_unit",
            columns=["id", "revision"],
            where=dict(revision__gt=0)),
        "subs_by_unit": dict(
            table="pootle_app_submission",
            columns=["unit_id", "creation_time", "id"],
            order_by=["unit_id", "-creation_time", "-id"],
            where=dict(unit_id__gt=0)),
        "subs_data": dict(
            table="pootle_app_submission",
            where=dict(unit_id__gt=0))}
    schema = {
        "submission": dict(
            table="pootle_app_submission",
            columns=["column_name"],
            optionally_enclose=None)}

    @property
    def data(self):
        return {
            "unit_revisions": dict(
                loader=self.revision_loader,
                ordered=True),
            "submission": dict(
                schema=True,
                ordered=True)}

    def del_revisions(self):
        del self.parsed["unit_revisions"]

    @property
    def mangle(self):
        reader_kwargs = dict(
            lineterminator='\n',
            escapechar="\\",
            doublequote=False)
        writer_kwargs = dict(
            lineterminator='\n',
            escapechar="\\",
            quotechar="'",
            doublequote=False,
            quoting=csv.QUOTE_NONE)
        return OrderedDict(
            (("revisions",
              dict(
                  source="subs_by_unit",
                  target="new_revisions",
                  mangler=self.revision_mangler,
                  after=self.del_revisions)),
             ("submissions", dict(
                 source="subs_data",
                 target="new_submissions",
                 parse=["new_revisions"],
                 mangler=self.submission_mangler,
                 reader_class=UnicodeCSVReader,
                 reader_kwargs=reader_kwargs,
                 writer_class=MySqlCSVWriter,
                 writer_kwargs=writer_kwargs))))
    # create = {
    #    "submission_temp": dict(source="pootle_app_submission")}
    alter = {
        "pootle_app_submission": ["remove", ["similarity", "mt_similarity"]]}
    load = {
        "new_submissions": dict(
            table="pootle_app_submission",
            force=True)}
    # validate = {
    #    "submission_temp": dict(
    #        model=Submission,
    #        columns=["id", "old_value", "new_value"])}

    def revision_loader(self, unit, revision):
        return (
            (unit, int(revision))
            if (int(revision) > 0)
            else None)

    @cached_property
    def revision_counter(self):
        class RevisionCounter(object):
            last_unitid = 0
            revision = None
        return RevisionCounter()

    def revision_mangler(self, unitid, creation_time, pk):
        """sets the revision for the last sub of each unit"""
        if self.revision_counter.last_unitid != unitid:
            self.revision_counter.revision = self.parsed[
                "unit_revisions"].get(unitid, 0)
        elif self.revision_counter.revision != 0:
            self.revision_counter.revision = None
        self.revision_counter.last_unitid = unitid
        if self.revision_counter.revision > 0:
            return [pk, self.revision_counter.revision]

    @property
    def submeta(self):
        return self.parsed.get("schema__submission")

    @property
    def subfields(self):
        return [
            "id", "creation_time", "unit_id",
            "old_value", "new_value",
            "similarity", "mt_similarity", "revision"]

    @cached_property
    def indices(self):
        keys = self.submeta.keys()
        return {
            k: keys.index(k)
            for k in self.subfields
            if k in keys}

    @cached_property
    def removed_indices(self):
        return [
            self.indices["similarity"],
            self.indices["mt_similarity"]]

    @cached_property
    def string_indices(self):
        return [
            self.indices["old_value"],
            self.indices["new_value"]]

    keep_zombies = False

    def submission_mangler(self, *sub):
        assert len(sub) == len(self.submeta)
        ignore = (
            not self.keep_zombies
            and sub[self.indices["unit_id"]] == "N")
        if ignore:
            return
        subid = sub[self.indices["id"]]
        revision = self.parsed["new_revisions"].get(subid)
        _sub = []
        for i, col in enumerate(sub):
            if i in self.removed_indices:
                continue
            if revision and i == self.indices["revision"]:
                _sub.append(revision)
            elif col == "N" and i not in self.string_indices:
                _sub.append(u"☠")
            else:
                _sub.append(col)
        _sub[self.indices["creation_time"]] = (
            '"%s"'
            % _sub[self.indices["creation_time"]])
        _sub[self.indices["old_value"]] = (
            u'"%s"'
            % _sub[self.indices["old_value"]].replace('"', u"☠\""))
        _sub[self.indices["new_value"]] = (
            u'"%s"'
            % _sub[self.indices["new_value"]].replace('"', u"☠\""))
        return _sub
