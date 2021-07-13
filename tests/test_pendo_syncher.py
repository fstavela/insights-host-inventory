import json
from unittest import mock

from requests import exceptions

from app import db
from app.config import Config
from app.environment import RuntimeEnvironment
from pendo_syncher import run as pendo_syncher_run
from tests.helpers.test_utils import MockResponseObject


def test_pendo_syncher_request_body(mocker, db_create_host):
    mock_request_function_mock = mocker.patch("lib.pendo_sync_helpers._make_request")

    host = db_create_host()

    request_body = [{"accountId": host.account, "values": {"hostCount": 1}}]
    request_body = json.dumps(request_body)

    config = Config(RuntimeEnvironment.PENDO_JOB)
    config.pendo_sync_active = True

    pendo_syncher_run(config, mock.Mock(), db.session, shutdown_handler=mock.Mock(**{"shut_down.return_value": False}))

    mock_request_function_mock.assert_called_once_with(request_body, config, mock.ANY)


def test_pendo_syncher_response_process(mocker, db_create_host):
    db_create_host()

    response_content = {"total": 1, "updated": 1, "failed": 0}

    mock_response = MockResponseObject()
    mock_response.status_code = 200
    mock_response.content = json.dumps(response_content)

    request_session_post_mock = mocker.patch("lib.pendo_sync_helpers.Session.post")
    request_session_post_mock.side_effect = mock_response

    config = Config(RuntimeEnvironment.PENDO_JOB)
    config.pendo_sync_active = True

    mock_pendo_failure = mocker.patch("lib.pendo_sync_helpers.pendo_failure")
    pendo_syncher_run(config, mock.Mock(), db.session, shutdown_handler=mock.Mock(**{"shut_down.return_value": False}))

    mock_pendo_failure.assert_not_called()


def test_pendo_syncher_response_process_failure(mocker, db_create_host):
    db_create_host()

    mock_response = MockResponseObject()
    mock_response.status_code = 403

    request_session_post_mock = mocker.patch("lib.pendo_sync_helpers.Session.post")
    request_session_post_mock.side_effect = mock_response

    config = Config(RuntimeEnvironment.PENDO_JOB)
    config.pendo_sync_active = True

    mock_pendo_failure = mocker.patch("lib.pendo_sync_helpers.pendo_failure")
    pendo_syncher_run(config, mock.Mock(), db.session, shutdown_handler=mock.Mock(**{"shut_down.return_value": False}))

    mock_pendo_failure.assert_called_once()


def test_pendo_syncher_exception(mocker, db_create_host):
    request_session_post_mock = mocker.patch("lib.pendo_sync_helpers.Session.post")
    request_session_post_mock.side_effect = Exception()

    db_create_host()

    config = Config(RuntimeEnvironment.PENDO_JOB)
    config.pendo_sync_active = True

    mock_pendo_failure = mocker.patch("lib.pendo_sync_helpers.pendo_failure")
    pendo_syncher_run(config, mock.Mock(), db.session, shutdown_handler=mock.Mock(**{"shut_down.return_value": False}))

    mock_pendo_failure.assert_called_once()


def test_pendo_syncher_retry_error(mocker, db_create_host):
    request_session_post_mock = mocker.patch("lib.pendo_sync_helpers.Session.post")
    request_session_post_mock.side_effect = exceptions.RetryError

    db_create_host()

    config = Config(RuntimeEnvironment.PENDO_JOB)
    config.pendo_sync_active = True

    mock_pendo_failure = mocker.patch("lib.pendo_sync_helpers.pendo_failure")
    pendo_syncher_run(config, mock.Mock(), db.session, shutdown_handler=mock.Mock(**{"shut_down.return_value": False}))

    mock_pendo_failure.assert_called_once()
