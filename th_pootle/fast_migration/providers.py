# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

from pootle.core.plugin import provider

from th_pootle.delegate import fast_migration

from th_pootle.fast_migration.p_store import (
    UnitChangeMigration0037, UnitSourceCreatedByMigration0033,
    UnitMigration0038)
from th_pootle.fast_migration.p_statistics import StatisticsMigration0011


@provider(fast_migration)
def fast_migration_provider(**kwargs_):
    return dict(
        pootle_statistics_0011=StatisticsMigration0011,
        pootle_store_0033=UnitSourceCreatedByMigration0033,
        pootle_store_0037=UnitChangeMigration0037,
        pootle_store_0038=UnitMigration0038)
