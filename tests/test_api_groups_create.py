import json
from copy import deepcopy

import pytest
from dateutil import parser

from app.auth.identity import Identity
from app.auth.identity import to_auth_header
from tests.helpers.api_utils import GROUP_WRITE_PROHIBITED_RBAC_RESPONSE_FILES
from tests.helpers.api_utils import assert_group_response
from tests.helpers.api_utils import assert_response_status
from tests.helpers.api_utils import create_mock_rbac_response
from tests.helpers.test_utils import SYSTEM_IDENTITY
from tests.helpers.test_utils import USER_IDENTITY
from tests.helpers.test_utils import generate_uuid


def test_create_group_with_empty_host_list(api_create_group, db_get_group_by_name, event_producer, mocker):
    mocker.patch.object(event_producer, "write_event")
    group_data = {"name": "my_awesome_group", "host_ids": []}

    response_status, response_data = api_create_group(group_data)

    assert_response_status(response_status, expected_status=201)

    retrieved_group = db_get_group_by_name(group_data.get("name"))
    assert_group_response(response_data, retrieved_group, 0)

    # No hosts modified, so no events should be written.
    assert event_producer.write_event.call_count == 0


def test_create_group_without_hosts_in_request_body(api_create_group, db_get_group_by_name, event_producer, mocker):
    mocker.patch.object(event_producer, "write_event")
    group_data = {"name": "my_awesome_group"}

    response_status, response_data = api_create_group(group_data)

    assert_response_status(response_status, expected_status=201)

    retrieved_group = db_get_group_by_name(group_data.get("name"))
    assert_group_response(response_data, retrieved_group, 0)

    # No hosts modified, so no events should be written.
    assert event_producer.write_event.call_count == 0


def test_create_group_with_hosts(
    db_create_host, api_create_group, db_get_group_by_name, db_get_host, mocker, event_producer
):
    mocker.patch.object(event_producer, "write_event")
    host1 = db_create_host()
    host2 = db_create_host()
    host_id_list = [str(host1.id), str(host2.id)]

    group_data = {"name": "my_awesome_group", "host_ids": host_id_list}

    response_status, response_data = api_create_group(group_data)

    assert_response_status(response_status, expected_status=201)

    retrieved_group = db_get_group_by_name(group_data["name"])
    assert_group_response(response_data, retrieved_group, 2)
    assert event_producer.write_event.call_count == 2
    for call_arg in event_producer.write_event.call_args_list:
        event = json.loads(call_arg[0][0])
        host = event["host"]
        assert host["id"] in host_id_list
        assert host["groups"][0]["name"] == group_data["name"]
        assert host["groups"][0]["id"] == str(retrieved_group.id)
        assert parser.isoparse(host["updated"]) == db_get_host(host["id"]).modified_on
        assert event["platform_metadata"] == {"b64_identity": to_auth_header(Identity(obj=USER_IDENTITY))}


@pytest.mark.parametrize(
    "new_name",
    [
        "",
        "  ",
    ],
)
def test_create_group_invalid_name(api_create_group, new_name):
    group_data = {"name": new_name, "host_ids": []}

    response_status, _ = api_create_group(group_data)

    assert_response_status(response_status, expected_status=400)


def test_create_group_null_name(api_create_group):
    group_data = {"host_ids": []}

    response_status, response_data = api_create_group(group_data)

    assert_response_status(response_status, expected_status=400)
    assert "Group name cannot be null" in response_data["detail"]


def test_create_group_read_only(api_create_group, mocker):
    group_data = {"name": "test", "host_ids": []}
    with mocker.patch("lib.middleware.get_flag_value", return_value=True):
        response_status, _ = api_create_group(group_data)
        assert_response_status(response_status, expected_status=503)


@pytest.mark.parametrize(
    "new_name",
    ["test_Group", " Test_Group", "test_group ", " test_group "],
)
def test_create_group_taken_name(api_create_group, new_name, mocker):
    mocker.patch(
        "lib.feature_flags.get_flag_value",
        side_effect=lambda name: name == "FLAG_INVENTORY_KESSEL_PHASE_1",
    )
    group_data = {"name": "test_group", "host_ids": []}

    api_create_group(group_data)
    group_data["name"] = new_name
    response_status, response_data = api_create_group(group_data)

    assert_response_status(response_status, expected_status=400)
    assert group_data["name"] in response_data["detail"]


@pytest.mark.usefixtures("event_producer")
@pytest.mark.parametrize(
    "new_name",
    ["test_Group", " Test_Group", "test_group ", " test_group "],
)
def test_create_group_taken_name_kessel(api_create_group, new_name, mocker):
    group_data = {"name": "test_group", "host_ids": []}

    api_create_group(group_data)
    group_data["name"] = new_name
    with mocker.patch("api.group.get_flag_value", return_value=True):
        response_status, response_data = api_create_group(group_data)

    assert_response_status(response_status, expected_status=400)
    assert group_data["name"] in response_data["detail"]


@pytest.mark.usefixtures("event_producer")
def test_create_group_taken_name_in_kessel_rbac(api_create_group, mocker):
    group_data = {"name": "test_group", "host_ids": []}

    error_message = "RBAC client error: Can't create workspace with same name within same parent workspace"
    with mocker.patch("api.group.get_flag_value", return_value=True):
        response_status, response_data = api_create_group(group_data, abort_status=400, abort_detail=error_message)

    assert_response_status(response_status, expected_status=400)
    assert_response_status(response_data["detail"], error_message)


@pytest.mark.parametrize(
    "host_ids",
    [["", "3578"], ["notauuid"]],
)
def test_create_group_invalid_host_ids(api_create_group, host_ids, event_producer, mocker):
    mocker.patch.object(event_producer, "write_event")
    group_data = {"name": "my_awesome_group", "host_ids": host_ids}

    response_status, response_data = api_create_group(group_data)

    assert_response_status(response_status, expected_status=400)
    assert any(s in response_data["detail"] for s in host_ids)

    # No hosts modified, so no events should be written.
    assert event_producer.write_event.call_count == 0


def test_create_group_with_host_from_another_group(
    db_create_group_with_hosts, db_get_hosts_for_group, api_create_group, event_producer, mocker
):
    mocker.patch.object(event_producer, "write_event")
    # Create a group with 2 hosts
    group = db_create_group_with_hosts("test_group", 2)
    hosts_in_group = db_get_hosts_for_group(group.id)
    assert len(hosts_in_group) == 2

    # Make an identity with a different org_id and account
    diff_identity = deepcopy(SYSTEM_IDENTITY)
    diff_identity["org_id"] = "diff_id"
    diff_identity["account"] = "diff_id"

    taken_host_id = str(hosts_in_group[0].id)

    group_data = {"name": "test_group_2", "host_ids": [taken_host_id]}

    response_status, response_body = api_create_group(group_data, identity=diff_identity)

    assert_response_status(response_status, 400)
    assert taken_host_id in response_body["detail"]

    # No hosts modified, so no events should be written.
    assert event_producer.write_event.call_count == 0


def test_create_group_with_host_from_another_org(db_create_host, api_create_group, event_producer, mocker):
    mocker.patch.object(event_producer, "write_event")
    host = db_create_host()
    host_id = str(host.id)

    # Make an identity with a different org_id and account
    diff_identity = deepcopy(SYSTEM_IDENTITY)
    diff_identity["org_id"] = "diff_id"
    diff_identity["account"] = "diff_id"

    group_data = {"name": "test_group_2", "host_ids": [host_id]}

    response_status, response_body = api_create_group(group_data, identity=diff_identity)

    assert_response_status(response_status, 400)
    assert host_id in response_body["detail"]

    # No hosts modified, so no events should be written.
    assert event_producer.write_event.call_count == 0


@pytest.mark.usefixtures("enable_rbac")
def test_create_group_RBAC_denied(subtests, mocker, api_create_group, db_get_group_by_name):
    get_rbac_permissions_mock = mocker.patch("lib.middleware.get_rbac_permissions")
    group_data = {"name": "my_awesome_group", "host_ids": []}

    for response_file in GROUP_WRITE_PROHIBITED_RBAC_RESPONSE_FILES:
        mock_rbac_response = create_mock_rbac_response(response_file)

        with subtests.test():
            get_rbac_permissions_mock.return_value = mock_rbac_response

            response_status, _ = api_create_group(group_data)

            assert_response_status(response_status, 403)

            assert not db_get_group_by_name("my_awesome_group")


@pytest.mark.usefixtures("enable_rbac")
def test_create_group_RBAC_denied_attribute_filter(mocker, api_create_group):
    get_rbac_permissions_mock = mocker.patch("lib.middleware.get_rbac_permissions")

    # Mock the RBAC response with a groups-write permission that has an attributeFilter
    mock_rbac_response = create_mock_rbac_response(
        "tests/helpers/rbac-mock-data/inv-groups-write-resource-defs-template.json"
    )
    mock_rbac_response[0]["resourceDefinitions"][0]["attributeFilter"]["value"] = [generate_uuid(), generate_uuid()]
    get_rbac_permissions_mock.return_value = mock_rbac_response

    group_data = {"name": "my_awesome_group", "host_ids": []}
    response_status, _ = api_create_group(group_data)

    # Access denied because of the attributeFilter
    assert_response_status(response_status, 403)
