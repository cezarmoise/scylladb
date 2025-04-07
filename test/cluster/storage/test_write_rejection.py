#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import logging
import pytest
import psutil
from test.cluster.util import new_test_keyspace, new_test_table
from cassandra.cluster import ConsistencyLevel, EXEC_PROFILE_DEFAULT

logger = logging.getLogger(__name__)

def write_generator(table, size_in_kb: int):
    for idx in range(size_in_kb):
        yield f"INSERT INTO {table} (pk, t) VALUES ({idx}, '{'x' * 1020}')"

@pytest.mark.asyncio
async def test_reject_writes(manager: ManagerClient) -> None:
    servers = await manager.all_servers()
    cql, hosts = await manager.get_ready_cql(servers)

    workdir = await manager.server_get_workdir(servers[0].server_id)
    disk_info = psutil.disk_usage(workdir)

    logger.info("Start adding more data")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        for server in servers:
            await manager.api.disable_autocompaction(server.ip_addr, ks)

        async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
            count = disk_info.free // 1024
            for query in write_generator(cf, count):
                cql.execute(query)

            profile = cql.execution_profile_clone_update(EXEC_PROFILE_DEFAULT, consistency_level = ConsistencyLevel.ONE)
            res = cql.execute(f"SELECT * from {cf} where pk = {count-1};", host=hosts[0], execution_profile=profile)
            assert res.one() is None

            for host in hosts[1:]:
                res = cql.execute(f"SELECT * from {cf} where pk = {count-1};", host=host, execution_profile=profile)
                assert res.one()
