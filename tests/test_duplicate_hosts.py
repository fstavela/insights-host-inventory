from unittest import mock
import pytest

from app import db
from app import threadctx
from app import UNKNOWN_REQUEST_ID_VALUE
from host_delete_duplicates import run as host_delete_duplicates_run
from host_delete_duplicates import main as host_delete_duplicates_main
from tests.helpers.db_utils import minimal_db_host
from tests.helpers.mq_utils import assert_synchronize_event_is_valid
from tests.helpers.test_utils import get_staleness_timestamps
from tests.helpers.test_utils import generate_uuid


# TODO: new tests needed.  these are from host_synchrnonizer
@pytest.mark.host_delete_duplicates
def test_delete_duplicate_host(
    event_producer_mock, event_datetime_mock, db_create_host, db_get_host, inventory_config
):
    staleness_timestamps = get_staleness_timestamps()

    host = minimal_db_host(stale_timestamp=staleness_timestamps["culled"], reporter="some reporter")
    created_host = db_create_host(host=host)

    assert db_get_host(created_host.id)

    threadctx.request_id = UNKNOWN_REQUEST_ID_VALUE
    host_delete_duplicates_run(
        inventory_config,
        mock.Mock(),
        db.session,
        event_producer_mock,
        shutdown_handler=mock.Mock(**{"shut_down.return_value": False}),
    )

    # check if host exist thought event synchronizer must find it to produce an update event.
    assert db_get_host(created_host.id)

    assert_synchronize_event_is_valid(
        event_producer=event_producer_mock, key=str(created_host.id), host=created_host, timestamp=event_datetime_mock
    )

    assert True


@pytest.mark.host_delete_duplicates
def test_delete_multiple_duplicate_hosts(event_producer, kafka_producer, db_create_multiple_hosts, inventory_config):
    host_count = 25

    db_create_multiple_hosts(how_many=host_count)

    threadctx.request_id = UNKNOWN_REQUEST_ID_VALUE
    inventory_config.script_chunk_size = 3

    event_count = host_delete_duplicates_run(
        inventory_config,
        mock.Mock(),
        db.session,
        event_producer,
        shutdown_handler=mock.Mock(**{"shut_down.return_value": False}),
    )

    assert event_count == host_count
    assert event_producer._kafka_producer.send.call_count == host_count

    assert True


@pytest.mark.host_delete_duplicates
def test_delete_duplicates_customer_scenario_1(event_producer, kafka_producer, db_create_host, db_get_host, inventory_config):
    # deleted_hosts_count = host_delete_duplicates_run(
    #     inventory_config,
    #     mock.Mock(),
    #     db.session,
    #     db.session,
    #     db.session,
    #     event_producer,
    #     shutdown_handler=mock.Mock(**{"shut_down.return_value": False}),
    # )  # <-- Works fine
    staleness_timestamps = get_staleness_timestamps()

    rhsm_id = generate_uuid()
    bios_uuid = generate_uuid()
    canonical_facts = {
        "insights_id": generate_uuid(),
        "subscription_manager_id": rhsm_id,
        "bios_uuid": bios_uuid,
        "satellite_id": rhsm_id,
        "fqdn": "rn001018",
        "ip_addresses": ["10.230.230.3"],
        "mac_addresses": ["00:50:56:ab:5a:22", "00:00:00:00:00:00"]
    }
    host_data = {
        "stale_timestamp": staleness_timestamps["stale_warning"],
        "reporter": "puptoo",
        "canonical_facts": canonical_facts
    }
    host1 = minimal_db_host(**host_data)
    created_host1 = db_create_host(host=host1)
    # deleted_hosts_count = host_delete_duplicates_run(
    #     inventory_config,
    #     mock.Mock(),
    #     db.session,
    #     db.session,
    #     db.session,
    #     event_producer,
    #     shutdown_handler=mock.Mock(**{"shut_down.return_value": False}),
    # )  # <-- Works fine

    host_data["canonical_facts"]["ip_addresses"] = ["10.230.230.30"]
    host_data["canonical_facts"].pop("bios_uuid")
    host_data["stale_timestamp"] = staleness_timestamps["stale"]
    host2 = minimal_db_host(**host_data)
    created_host2 = db_create_host(host=host2)
    # deleted_hosts_count = host_delete_duplicates_run(
    #     inventory_config,
    #     mock.Mock(),
    #     db.session,
    #     db.session,
    #     db.session,
    #     event_producer,
    #     shutdown_handler=mock.Mock(**{"shut_down.return_value": False}),
    # )  # <-- Works fine

    host_data["canonical_facts"]["ip_addresses"] = ["10.230.230.3"]
    host3 = minimal_db_host(**host_data)
    created_host3 = db_create_host(host=host3)
    # deleted_hosts_count = host_delete_duplicates_run(
    #     inventory_config,
    #     mock.Mock(),
    #     db.session,
    #     db.session,
    #     db.session,
    #     event_producer,
    #     shutdown_handler=mock.Mock(**{"shut_down.return_value": False}),
    # )  # <-- Raises an error

    host_data["reporter"] = "yupana"
    host_data["canonical_facts"]["ip_addresses"] = ["10.230.230.1"]
    host_data["canonical_facts"]["mac_addresses"] = ["00:50:56:ab:5a:22"]
    host_data["canonical_facts"]["bios_uuid"] = bios_uuid
    host_data["canonical_facts"]["fqdn"] = "rn001018.bcbst.com"
    host_data["stale_timestamp"] = staleness_timestamps["fresh"]
    host4 = minimal_db_host(**host_data)
    created_host4 = db_create_host(host=host4)
    # deleted_hosts_count = host_delete_duplicates_run(
    #     inventory_config,
    #     mock.Mock(),
    #     db.session,
    #     db.session,
    #     db.session,
    #     event_producer,
    #     shutdown_handler=mock.Mock(**{"shut_down.return_value": False}),
    # )

    host_data["reporter"] = "puptoo"
    host_data["canonical_facts"]["ip_addresses"] = ["10.230.230.15"]
    host_data["canonical_facts"]["mac_addresses"] = ["00:50:56:ab:5a:22", "00:00:00:00:00:00"]
    host_data["canonical_facts"].pop("bios_uuid")
    host_data["canonical_facts"]["fqdn"] = "rn001018"
    host5 = minimal_db_host(**host_data)
    created_host5 = db_create_host(host=host5)

    assert db_get_host(created_host1.id)
    assert db_get_host(created_host2.id)
    assert db_get_host(created_host3.id)
    assert db_get_host(created_host4.id)
    assert db_get_host(created_host5.id)

    # deleted_hosts_count = host_delete_duplicates_run(
    #     inventory_config,
    #     mock.Mock(),
    #     db.session,
    #     db.session,
    #     db.session,
    #     event_producer,
    #     shutdown_handler=mock.Mock(**{"shut_down.return_value": False}),
    # )
    # assert deleted_hosts_count == 4
    host_delete_duplicates_main(mock.Mock())
    # assert not db_get_host(created_host1.id)
    # assert not db_get_host(created_host2.id)
    # assert not db_get_host(created_host3.id)
    # assert not db_get_host(created_host4.id)
    assert db_get_host(created_host5.id)


@pytest.mark.host_delete_duplicates
def test_delete_duplicates_customer_scenario_2(event_producer, kafka_producer, db_create_host, db_get_host, inventory_config):
    staleness_timestamps = get_staleness_timestamps()

    rhsm_id = generate_uuid()
    bios_uuid = generate_uuid()
    canonical_facts = {
        "insights_id": generate_uuid(),
        "subscription_manager_id": rhsm_id,
        "bios_uuid": bios_uuid,
        "satellite_id": rhsm_id,
        "fqdn": "rozrhjrad01.base.srvco.net",
        "ip_addresses": ["10.230.230.10", "10.230.230.13"],
        "mac_addresses": ["00:50:56:ac:56:45", "00:50:56:ac:48:61", "00:00:00:00:00:00"]
    }
    host_data = {
        "stale_timestamp": staleness_timestamps["stale_warning"],
        "reporter": "puptoo",
        "canonical_facts": canonical_facts
    }
    host1 = minimal_db_host(**host_data)
    created_host1 = db_create_host(host=host1)

    host_data["canonical_facts"]["ip_addresses"] = ["10.230.230.3", "10.230.230.4"]
    host2 = minimal_db_host(**host_data)
    created_host2 = db_create_host(host=host2)

    host_data["canonical_facts"]["ip_addresses"] = ["10.230.230.1", "10.230.230.4"]
    host_data["stale_timestamp"] = staleness_timestamps["fresh"]
    host3 = minimal_db_host(**host_data)
    created_host3 = db_create_host(host=host3)

    assert db_get_host(created_host1.id)
    assert db_get_host(created_host2.id)
    assert db_get_host(created_host3.id)

    # deleted_hosts_count = host_delete_duplicates_run(
    #     inventory_config,
    #     mock.Mock(),
    #     db.session,
    #     db.session,
    #     db.session,
    #     event_producer,
    #     shutdown_handler=mock.Mock(**{"shut_down.return_value": False}),
    # )
    # assert deleted_hosts_count == 2
    assert not db_get_host(created_host1.id)
    assert not db_get_host(created_host2.id)
    assert db_get_host(created_host3.id)
