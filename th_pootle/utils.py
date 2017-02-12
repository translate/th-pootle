# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

import codecs
import contextlib
import csv
import cStringIO
import os
import time
import uuid
from collections import OrderedDict

from django import db
from django.utils.functional import cached_property


class UTF8CSVRecoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")


class UnicodeCSVReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8CSVRecoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self


class MySqlCSVWriter:

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        data = data.replace("__☠__", b'\N')
        data = data.replace("\\n", b'\n')
        data = data.replace('\\"', b'\"')
        data = data.replace("\\,", ",")
        data = data.replace("\\\\", "\\")
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class FastMigration(object):

    def fpath(self, path, name, k):
        return os.path.join(
            path, "%s.%s.txt" % (k, name))

    def migrate(self, actions=None, name=None, path="/tmp"):
        name = name or uuid.uuid4().hex[:10]
        if not actions or "dump" in actions:
            for dumped in self.dump_data(path, name):
                yield "dump", dumped
        if not actions or "schema" in actions:
            for schema in self.dump_schema(path, name):
                yield "schema", schema
        if not actions or "mangle" in actions:
            for parsed in self.parse_data(path, name):
                yield "parse", parsed
            for mangled in self.mangle_data(path, name):
                yield mangled["action"], mangled
        elif "parse" in actions:
            for parsed in self.parse_data(path, name):
                yield "parse", parsed
        if not actions or "create" in actions:
            for created in self.create_data(path, name):
                yield "create", created
        if not actions or "alter" in actions:
            for altered in self.alter_data(path, name):
                yield "alter", altered
        if not actions or "load" in actions:
            for loaded in self.load_data(path, name):
                yield "load", loaded
        if actions and "validate" in actions:
            for validated in self.validate_data(path, name):
                yield "validate", validated

    def dump_data(self, path, name):
        for k, dump in self.dump.items():
            yield self.outfile.select(
                dump.pop("table"),
                self.fpath(path, name, k),
                dump.pop("columns", None),
                **dump)

    def dump_schema(self, path, name):
        for k, dump in self.schema.items():
            yield self.outfile.select_schema(
                dump.pop("table"),
                self.fpath(path, name, "schema__%s" % k),
                dump.pop("columns", None),
                **dump)

    def parse_data(self, path, name):
        self.parsed = {}
        for k, data in self.data.items():
            if data.pop("schema", None):
                k = "schema__%s" % k
            result = self.csv_loader(
                self.fpath(path, name, k)).as_dict(
                    loader=data.pop("loader", None),
                    **data)
            result["name"] = k
            self.parsed[k] = result["results"]
            del result["results"]
            yield result

    def mangle_data(self, path, name):
        for k, mangle in self.mangle.items():
            target = mangle.pop("target", None)
            after = mangle.pop("after", None)
            parse_extra = mangle.pop("parse", [])
            for parse in parse_extra:
                result = self.csv_loader(
                    self.fpath(path, name, parse)).as_dict()
                result["action"] = "parse"
                result["name"] = parse
                self.parsed[parse] = result["results"]
                del result["results"]
                yield result
            if target:
                target = self.fpath(path, name, target)
            mangler = self.csv_mangler(
                self.fpath(path, name, mangle.pop("source")),
                target,
                **mangle)
            mangled = mangler.mangle(mangle["mangler"])
            mangled["name"] = k
            mangled["action"] = "mangle"
            if after:
                after()
            if not target:
                self.parsed["mangled__%s" % k] = mangled
            yield mangled

    def create_data(self, path, name):
        for k, create in self.create.items():
            source = create.pop("source")
            if self.manager.rowcount(source) is None:
                result = self.manager.copy_table(
                    source, k)
                yield result

    def alter_data(self, path, name):
        for k, alter in self.alter.items():
            start = time.time()
            action = alter[0]
            if action == "remove":
                try:
                    result = self.manager.drop_columns(k, alter[1])
                except db.OperationalError:
                    # most likely the column doesnt exist
                    pass
            yield dict(
                table=k,
                result=result,
                columns=alter[1],
                timing=(time.time() - start))

    def load_data(self, path, name):
        for k, load in self.load.items():
            result = self.infile.load(
                load.pop("table"),
                self.fpath(path, name, k),
                local=load.pop("local", False),
                **load)
            yield result

    def validate_data(self, path, name):
        start = time.time()
        for k, validate in self.validate.items():
            check_count = validate.get("check_count", False)
            step = validate.get("step", 10000)
            self.cursor.execute("select count(*) from %s" % k)
            total = self.cursor.fetchone()[0]
            if check_count:
                assert validate["model"].objects.count() == total
            i = 0
            columns = validate.get("columns", ["*"])
            while True:
                self.cursor.execute(
                    "select %s from %s limit %s offset %s"
                    % (", ".join(columns), k, step, i + step))
                result = self.cursor.fetchall()
                existing = tuple(
                    validate["model"].objects.filter(
                        id__in=[
                            res[0]
                            for res
                            in result]).values_list(*columns))
                existing = {e[0]: e[1:] for e in existing}
                result = {r[0]: r[1:] for r in result}
                assert existing == result
                i += step
                yield dict(
                    action="validate",
                    table=k,
                    columns=", ".join(columns),
                    model=str(validate["model"]._meta),
                    count=min(i, total),
                    total=total,
                    timing=(time.time() - start))
                if i > total:
                    break

    @cached_property
    def cursor(self):
        return db.connection.cursor()

    @cached_property
    def infile(self):
        return MySqlInfile(self, self.cursor)

    @cached_property
    def outfile(self):
        return MySqlOutfile(self, self.cursor)

    @cached_property
    def manager(self):
        return MySqlManager(self, self.cursor)

    @property
    def csv_loader(self):
        return CSVLoader

    @property
    def csv_mangler(self):
        return CSVMangler


class CSVLoader(object):

    def __init__(self, source, **kwargs):
        self.source = source
        self.kwargs = kwargs

    def as_dict(self, loader=None, ordered=False):
        start = time.time()
        results = (
            OrderedDict()
            if ordered
            else {})
        total = 0
        with open(self.source, "rb") as f:
            parsed_data = csv.reader(f)
            for row in parsed_data:
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
                elif row:
                    results[row[0]] = None
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
        if self.target:
            return self.mangle_to_file(mangler)
        return self.mangle_to_mem(mangler)

    def mangle_to_mem(self, mangler):
        start = time.time()
        count = 0
        total = 0
        results = []
        for result in self.iter_source(mangler):
            total += 1
            if result is not None:
                count += 1
                results.append(result)
        return dict(
            source=self.source,
            target=":memory:",
            count=count,
            results=results,
            total=total,
            timing=(time.time() - start))

    def iter_source(self, mangler):
        with open(self.source, "rb") as rf:
            reader = self.reader(rf, **self.reader_kwargs)
            for row in reader:
                yield mangler(*row)

    def mangle_to_file(self, mangler):
        start = time.time()
        count = 0
        total = 0
        with open(self.target, "wb") as wf:
            writer = self.writer(wf, **self.writer_kwargs)
            for result in self.iter_source(mangler):
                total += 1
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
def fast_load(cursor):
    try:
        cursor.execute("SET GLOBAL sync_binlog=0;")
        cursor.execute("SET GLOBAL innodb_flush_log_at_trx_commit=2;")
        cursor.execute("SET UNIQUE_CHECKS=0;")
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        cursor.execute("SET SESSION tx_isolation='READ-UNCOMMITTED';")
        yield
    finally:
        cursor.execute("SET SESSION tx_isolation='REPEATABLE-READ';")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        cursor.execute("SET UNIQUE_CHECKS=1;")
        cursor.execute("SET GLOBAL innodb_flush_log_at_trx_commit=1;")
        cursor.execute("SET GLOBAL sync_binlog=1;")


@contextlib.contextmanager
def no_fk_checks(cursor):
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        yield
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")


@contextlib.contextmanager
def table_lock(cursor, table):
    try:
        cursor.execute("LOCK TABLE %s WRITE;" % table)
        yield
    finally:
        cursor.execute("UNLOCK TABLES;")


class MySqlBase(object):

    def execute(self, sql):
        print sql
        return self.cursor.execute(sql)


class MySqlManager(MySqlBase):

    def __init__(self, context, cursor, **kwargs):
        self.context = context
        self.cursor = cursor

    def truncate_table(self, table):
        with no_fk_checks(self.cursor):
            self.cursor.execute(
                "truncate table %s; "
                % table)

    def drop_columns(self, table, columns):
        column_sql = ", ".join(
            "DROP COLUMN %s" % c for c in columns)
        self.cursor.execute(
            "alter table %s %s"
            % (table, column_sql))

    def copy_table(self, source, target):
        self.cursor.execute(
            "CREATE TABLE %s LIKE "
            "%s;"
            % (target, source))

    def rowcount(self, table):
        try:
            self.cursor.execute(
                "select count(*) from %s" % table)
        except db.ProgrammingError:
            return None
        return self.cursor.fetchone()[0]


class MySqlFile(MySqlBase):

    field_terminator = ","
    line_terminator = "\\n"
    optionally_enclose = '"'

    def __init__(self, context, cursor, **kwargs):
        self.context = context
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
        return ", ".join("`%s`" % c for c in columns)

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
            oper = " = "
            if k.endswith("__gt"):
                k = k[:-4]
                oper = " > "
            if k.endswith("__ne"):
                k = k[:-4]
                oper = " != "
            clauses.append("%s%s'%s'" % (k, oper, v))
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
            table=table,
            rows=rows,
            timing=(time.time() - start))

    def select_schema(self, table, outfile, columns, **kwargs):
        start = time.time()
        table_name = table
        table = "information_schema.COLUMNS"
        rows = self.execute(
            self.generate_sql(
                table,
                outfile,
                self.get_columns(columns),
                where=dict(
                    table_name=table_name,
                    table_schema=self.dbname),
                **kwargs))
        return dict(
            filepath=outfile,
            rows=rows,
            table=table,
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
            "ESCAPED BY '\\\\' "
            "%s"
            "LINES TERMINATED BY '%s'; "
            % (self.get_local(local),
               infile,
               table,
               self.field_terminator,
               self.get_enclosed_by(**kwargs),
               self.line_terminator))

    def load(self, table, infile, **kwargs):
        start = time.time()
        count = self.context.manager.rowcount(table)
        if count and not kwargs.get("force"):
            raise db.IntegrityError(
                "Target table '%s' contains data and "
                "force not specificed" % table)
        elif count:
            self.context.manager.truncate_table(table)
        elif count is None:
            if kwargs.get("create", False):
                self.context.manager.copy_table(
                    kwargs.pop("source_table"), table)
            else:
                raise db.OperationalError(
                    "Target table '%s' does not exist "
                    "and create not specificed" % table)

        with table_lock(self, table):
            with fast_load(self):
                rows = self.execute(
                    self.generate_sql(
                        table,
                        infile,
                        kwargs.get("local", False)))
        return dict(
            filepath=infile,
            rows=rows,
            table=table,
            timing=(time.time() - start))
