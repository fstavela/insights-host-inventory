from sqlalchemy import and_
from sqlalchemy import not_
from sqlalchemy import or_

from app.models import Host

__all__ = ("delete_duplicate_hosts",)

CANONICAL_FACTS = ("fqdn", "satellite_id", "bios_uuid", "ip_addresses", "mac_addresses")
ELEVATED_CANONICAL_FACT_FIELDS = ("provider_id", "insights_id", "subscription_manager_id")


def matches_at_least_one_canonical_fact_filter(canonical_facts):
    # Contains at least one correct CF value
    # Correct value = contains key:value
    # -> OR( *correct values )
    return or_(Host.canonical_facts.contains({key: value}) for key, value in canonical_facts.items())


def contains_no_incorrect_facts_filter(canonical_facts):
    # Does not contain any incorrect CF values
    # Incorrect value = AND( key exists, NOT( contains key:value ) )
    # -> NOT( OR( *Incorrect values ) )
    filter_ = ()
    for key, value in canonical_facts.items():
        filter_ += (
            and_(Host.canonical_facts.has_key(key), not_(Host.canonical_facts.contains({key: value}))),  # noqa: W601
        )

    return not_(or_(*filter_))


def multiple_canonical_facts_host_query(canonical_facts, query):
    query = query.filter(
        (contains_no_incorrect_facts_filter(canonical_facts))
        & (matches_at_least_one_canonical_fact_filter(canonical_facts))
    )
    return query


# Get hosts by the highest elevated canonical fact present
def find_host_by_elevated_canonical_facts(elevated_cfs, query, logger):
    """
    First check if multiple hosts are returned.  If they are then retain the one with the highest
    priority elevated fact
    """
    logger.debug("find_host_by_elevated_canonical_facts(%s)", elevated_cfs)

    if elevated_cfs.get("provider_id"):
        elevated_cfs.pop("subscription_manager_id", None)
        elevated_cfs.pop("insights_id", None)
    elif elevated_cfs.get("insights_id"):
        elevated_cfs.pop("subscription_manager_id", None)

    host = multiple_canonical_facts_host_query(elevated_cfs, query).order_by(Host.modified_on.desc()).first()

    if host:
        logger.debug("Found existing host using canonical_fact match")

    return host


# this function is called when no elevated canonical facts are present in the host
def find_host_by_regular_canonical_facts(canonical_facts, query, logger):
    """
    Returns all matches for a host containing given canonical facts
    """
    logger.debug("find_host_by_regular_canonical_facts(%s)", canonical_facts)

    host = multiple_canonical_facts_host_query(canonical_facts, query).order_by(Host.modified_on.desc()).first()

    if host:
        logger.debug("Found existing host using canonical_fact match")

    return host


def get_elevated_canonical_facts(canonical_facts):
    elevated_facts = {key: canonical_facts[key] for key in ELEVATED_CANONICAL_FACT_FIELDS if key in canonical_facts}
    return elevated_facts


def get_regular_canonical_facts(canonical_facts):
    regular_cfs = {key: canonical_facts[key] for key in CANONICAL_FACTS if key in canonical_facts}
    return regular_cfs


def _delete_hosts_by_id_list(session, host_id_list):
    delete_query = session.query(Host).filter(Host.id.in_(host_id_list))
    delete_query.delete(synchronize_session="fetch")
    delete_query.session.commit()


def delete_duplicate_hosts(accounts_session, hosts_session, misc_session, chunk_size, logger, interrupt=lambda: False):
    total_deleted = 0
    hosts_query = hosts_session.query(Host)
    account_query = accounts_session.query(Host.account)

    logger.info(f"Total number of hosts in inventory: {hosts_query.count()}")
    logger.info(f"Total number of accounts in inventory: {account_query.distinct(Host.account).count()}")

    for account in account_query.distinct(Host.account).yield_per(chunk_size):
        logger.info(f"Processing account {account}")
        # set uniquess within the account
        unique_list = []
        duplicate_list = []
        misc_query = misc_session.query(Host).filter(Host.account == account)

        def unique(host):
            if host.id not in unique_list:
                unique_list.append(host.id)
            else:
                duplicate_list.append(host.id)

            logger.info(f"Unique: {len(unique_list)} | Duplicate: {len(duplicate_list)}")

        for host in hosts_query.filter(Host.account == account).yield_per(chunk_size):
            logger.info(f"Host ID: {host.id}")
            logger.info(f"Canonical facts: {host.canonical_facts}")
            elevated_cfs = get_elevated_canonical_facts(host.canonical_facts)
            if elevated_cfs:
                logger.info(f"elevated canonical facts: {elevated_cfs}")
                host_match = find_host_by_elevated_canonical_facts(elevated_cfs, misc_query, logger)
            else:
                regular_cfs = get_regular_canonical_facts(host.canonical_facts)
                logger.info(f"regular canonical facts: {regular_cfs}")
                if regular_cfs:
                    host_match = find_host_by_regular_canonical_facts(regular_cfs, misc_query, logger)

            unique(host_match)

        # delete duplicate hosts
        _delete_hosts_by_id_list(misc_session, duplicate_list)
        total_deleted += len(duplicate_list)

    return total_deleted