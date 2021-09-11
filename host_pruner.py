import sys
from functools import partial

from prometheus_client import CollectorRegistry
from prometheus_client import push_to_gateway
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import UNKNOWN_REQUEST_ID_VALUE
from app.config import Config
from app.environment import RuntimeEnvironment
from app.logging import configure_logging
from app.logging import get_logger
from app.logging import threadctx
from app.models import Host
from app.queue.event_producer import EventProducer
from app.queue.metrics import event_producer_failure
from app.queue.metrics import event_producer_success
from app.queue.metrics import event_serialization_time
from lib.db import session_guard
from lib.delete_hosts import delete_duplicate_hosts
from lib.handlers import register_shutdown
from lib.handlers import ShutdownHandler
from lib.metrics import synchronize_fail_count
from lib.metrics import synchronize_host_count

__all__ = ("main", "run")

PROMETHEUS_JOB = "duplicate_hosts_pruner"
LOGGER_NAME = "duplicate_hosts_pruner"
COLLECTED_METRICS = (
    synchronize_host_count,
    synchronize_fail_count,
    event_producer_failure,
    event_producer_success,
    event_serialization_time,
)
RUNTIME_ENVIRONMENT = RuntimeEnvironment.JOB


def _init_config():
    config = Config(RUNTIME_ENVIRONMENT)
    config.log_configuration()
    return config


def _init_db(config):
    engine = create_engine(config.db_uri)
    return sessionmaker(bind=engine)


def _prometheus_job(namespace):
    return f"{PROMETHEUS_JOB}-{namespace}" if namespace else PROMETHEUS_JOB


def _excepthook(logger, type, value, traceback):
    logger.exception("Host synchronizer failed", exc_info=value)


@synchronize_fail_count.count_exceptions()
def run(config, logger, session, event_producer, shutdown_handler):

    query = session.query(Host)
    # query = session.query(Host.id, Host.canonical_facts)

    update_count = 0
    events = delete_duplicate_hosts(
        query, event_producer, config.script_chunk_size, config, shutdown_handler.shut_down
    )
    for host_id in events:
        logger.info("Deleted host: %s", host_id)
        update_count += 1
        logger.info(f"Number of hosts deleted: {update_count}")
    logger.info(f"Total number of hosts deleted: {update_count}")
    return update_count


def main(logger):

    config = _init_config()
    registry = CollectorRegistry()

    for metric in COLLECTED_METRICS:
        registry.register(metric)

    job = _prometheus_job(config.kubernetes_namespace)
    prometheus_shutdown = partial(push_to_gateway, config.prometheus_pushgateway, job, registry)
    register_shutdown(prometheus_shutdown, "Pushing metrics")

    Session = _init_db(config)
    session = Session()
    register_shutdown(session.get_bind().dispose, "Closing database")

    event_producer = EventProducer(config)
    register_shutdown(event_producer.close, "Closing producer")

    shutdown_handler = ShutdownHandler()
    shutdown_handler.register()

    with session_guard(session):
        run(config, logger, session, event_producer, shutdown_handler)


if __name__ == "__main__":
    configure_logging()

    logger = get_logger(LOGGER_NAME)
    sys.excepthook = partial(_excepthook, logger)

    threadctx.request_id = UNKNOWN_REQUEST_ID_VALUE
    main(logger)
