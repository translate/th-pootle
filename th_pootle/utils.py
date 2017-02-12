# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

import contextlib
import csv
import time


class CSVLoader(object):

    def __init__(self, source, **kwargs):
        self.source = source
        self.kwargs = kwargs

    def as_dict(self, loader=None):
        start = time.time()
        results = {}
        total = 0
        with open(self.source, "rb") as f:
            unit_data = csv.reader(f)
            for row in unit_data:
                total += 1
                if loader:
                    result = loader(*row)
                    if result is not None:
                        k, v = result
                        results[k] = v
                elif len(row) == 2:
                    results[row[0]] = row[1]
                elif len(row) > 2:
                    results[row[0]] = row[1:]
        return dict(
            source=self.source,
            total=total,
            count=len(results),
            results=results,
            timing=(time.time() - start))


class CSVMangler(object):

    def __init__(self, source, target, **kwargs):
        self.source = source
        self.target = target
        self.reader = kwargs.pop("reader_class", csv.reader)
        self.writer = kwargs.pop("writer_class", csv.writer)
        self.kwargs = kwargs

    @property
    def reader_kwargs(self):
        return self.kwargs.get("reader_kwargs", {})

    @property
    def writer_kwargs(self):
        return self.kwargs.get("writer_kwargs", {})

    def mangle(self, mangler):
        start = time.time()
        count = 0
        total = 0
        with open(self.target, "wb") as wf:
            writer = self.writer(wf, **self.writer_kwargs)
            with open(self.source, "rb") as rf:
                reader = self.reader(rf, **self.reader_kwargs)
                for row in reader:
                    total += 1
                    result = mangler(*row)
                    if result is not None:
                        count += 1
                        writer.writerow(result)
        return dict(
            source=self.source,
            target=self.target,
            count=count,
            total=total,
            timing=(time.time() - start))


@contextlib.contextmanager
def no_fk_checks(cursor):
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        yield
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")


class MySqlBase(object):

    def execute(self, sql):
        print sql
        return self.cursor.execute(sql)


class MySqlManager(MySqlBase):

    def __init__(self, cursor, **kwargs):
        self.cursor = cursor

    def truncate_table(self, table):
        with no_fk_checks(self.cursor):
            self.cursor.execute(
                "truncate table %s; "
                % table)

    def copy_table(self, source, target):
        self.cursor.execute(
            "create table %s like "
            "%s;"
            % (target, source))


class MySqlFile(MySqlBase):

    field_terminator = ","
    line_terminator = "\\n"
    optionally_enclose = '"'

    def __init__(self, cursor, **kwargs):
        self.cursor = cursor
        self.field_terminator = kwargs.get(
            "field_terminator", self.field_terminator)
        self.line_terminator = kwargs.get(
            "line_terminator", self.line_terminator)
        self.optionally_enclose = kwargs.get(
            "optionally_enclose", self.optionally_enclose)

    def get_enclosed_by(self, **kwargs):
        enclose = kwargs.get(
            "optionally_enclose",
            self.optionally_enclose)
        return (
            ("OPTIONALLY ENCLOSED BY '%s' "
             % enclose)
            if enclose
            else "")


class MySqlOutfile(MySqlFile):

    def get_columns(self, columns):
        if columns is None:
            return "*"
        return ", ".join(columns)

    @property
    def dbname(self):
        return self.cursor.db.settings_dict["NAME"]

    def get_order_by(self, order_by):
        if order_by is None:
            return ""

        def order(item):
            if item.startswith("-"):
                return "%s DESC" % item[1:]
            return item
        return (
            "ORDER BY %s "
            % (", ".join([order(o) for o in order_by])))

    def get_where(self, where):
        if where is None:
            return ""
        clauses = []
        for k, v in where.items():
            clauses.append("%s = '%s'" % (k, v))
        return "WHERE %s " % " AND ".join(clauses)

    def generate_sql(self, table, outfile, columns, **kwargs):
        return (
            "SELECT %s INTO OUTFILE '%s' "
            "FIELDS TERMINATED BY '%s' "
            "%s"
            "LINES TERMINATED BY '%s' "
            "FROM %s "
            "%s"
            "%s"
            % (columns,
               outfile,
               self.field_terminator,
               self.get_enclosed_by(**kwargs),
               self.line_terminator,
               table,
               self.get_where(kwargs.get("where")),
               self.get_order_by(kwargs.get("order_by"))))

    def select(self, table, outfile, columns=None, **kwargs):
        start = time.time()
        rows = self.execute(
            self.generate_sql(
                table,
                outfile,
                self.get_columns(columns),
                **kwargs))
        return dict(
            filepath=outfile,
            rows=rows,
            timing=(time.time() - start))

    def select_schema(self, table, outfile, **kwargs):
        start = time.time()
        table_name = table
        table = "information_schema.COLUMNS"
        rows = self.execute(
            self.generate_sql(
                table,
                outfile,
                self.get_columns(["column_name"]),
                where=dict(
                    table_name=table_name,
                    table_schema=self.dbname),
                **kwargs))
        return dict(
            filepath=outfile,
            rows=rows,
            timing=(time.time() - start))


class MySqlInfile(MySqlFile):

    def get_local(self, local):
        return (
            "LOCAL "
            if local
            else "")

    def generate_sql(self, table, infile, local, **kwargs):
        return (
            "LOAD DATA %sINFILE '%s' into table %s "
            "FIELDS TERMINATED BY '%s' "
            "%s"
            "LINES TERMINATED BY '%s' "
            % (self.get_local(local),
               infile,
               table,
               self.field_terminator,
               self.get_enclosed_by(**kwargs),
               self.line_terminator))

    def load(self, table, infile, **kwargs):
        start = time.time()
        rows = self.execute(
            self.generate_sql(
                table,
                infile,
                kwargs.get("local", False)))
        return dict(
            filepath=infile,
            rows=rows,
            timing=(time.time() - start))
