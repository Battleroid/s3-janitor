import argparse
import logging
import boto3
import sys
from botocore.client import Config
from dateutil.tz import tzutc
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(name)8s [%(levelname)-8s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)
log = logging.getLogger('s3-janitor')
logging.getLogger('botocore').setLevel(logging.WARN)


def purge(bucket, max_days, access_key, secret_key, endpoint,
        list_then_delete=True, dry_run=False):
    """
    Clears bucket a particular bucket using its own client.
    """

    plog_name = bucket.translate(str.maketrans({' ': '_', '.': '_'}))
    plog = log.getChild(f'{plog_name}_worker')

    boto_config = Config(connect_timeout=120, read_timeout=300)
    session = boto3.session.Session()
    client = session.client(
        service_name='s3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
        config=boto_config
    )

    cutoff = datetime.combine(
        date.today(),
        datetime.min.time(),
        tzinfo=tzutc()
    ) - timedelta(days=max_days)

    paginator = client.get_paginator('list_objects')
    bucket_iterator = paginator.paginate(Bucket=bucket)
    to_be_deleted = []

    # If we are deleting as we go, to_be_deleted will be recreated
    # with every page listed, if not, we will chunk to_be_deleted
    # later on
    total = 0
    for i, page in enumerate(bucket_iterator, 1):

        page_queue = []

        # Find all out of date objects
        for obj in page['Contents']:
            key = obj['Key']
            last_modified = obj['LastModified']

            if last_modified <= cutoff:
                plog.debug(f'{key} queued')
                page_queue.append(key)
                total += 1

        plog.info(f'Found {len(page_queue)} objects in chunk {i} for {bucket}')

        # Delete the page contents right now if applicable
        if not list_then_delete and bool(page_queue):
            plog.info(f'Deleting queue {i} ({len(page_queue)} objects)')
            if not dry_run:
                client.delete_objects(
                    Bucket=bucket,
                    Delete={
                        'Objects': [{'Key': o} for o in page_queue],
                        'Quiet': True
                    }
                )
            page_queue = []

        to_be_deleted += [*page_queue]

    # Delete what's leftover, or everything now depending on the method chosen
    if to_be_deleted:

        plog.info(f'Total objects queued for deletion {len(to_be_deleted)}')

        chunks = [
            to_be_deleted[i:i + 1000] for i in
            range(0, len(to_be_deleted), 1000)
        ]

        plog.info(f'{len(chunks)} chunks formed from {len(to_be_deleted)} objects')

        for i, chunk in enumerate(chunks, 1):
            plog.info(f'Deleting chunk {i}/{len(chunks)} ({len(chunk)} objects) for {bucket}')
            if not dry_run:
                client.delete_objects(
                    Bucket=bucket,
                    Delete={
                        'Objects': [{'Key': o} for o in chunk],
                        'Quiet': True
                    }
                )

    plog.info(f'Finished deletion of {total} objects')


def do(args):
    """
    Purge any objects in buckets that are older than period.
    """

    # Setup vars
    max_days = args.days
    dry_run = args.dry_run
    access_key = args.access_key
    secret_key = args.secret_key
    endpoint = args.endpoint
    target_method = args.target_method
    bucket_prefix = args.prefix
    bucket_targets = args.buckets
    list_then_delete = args.delete_method == 'list_then_delete'

    if dry_run:
        log.info('DRY RUN MODE: No deletions will be made')

    # We can't do anything if we can't find anything
    if target_method == 'prefix':
        if not bucket_prefix:
            raise SystemExit('No prefix provided')
    else:
        if not bucket_targets:
            raise SystemExit('No buckets provided')

    cutoff = (datetime.combine(
        date.today(),
        datetime.min.time(), 
        tzinfo=tzutc()
    ) - timedelta(days=max_days)).strftime('%Y-%m-%d')
    log.info(f'Searching for objects before cutoff date of {cutoff}')

    # Setup client to do initial bucket listing
    session = boto3.session.Session()
    client = session.client(
        service_name='s3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint
    )

    # Check if we have any buckets that match our preferred method
    if target_method == 'prefix':
        buckets = [
            b['Name'] for b in client.list_buckets()['Buckets']
            if b['Name'].startswith(bucket_prefix)
        ]
    else:
        buckets = [
            b['Name'] for b in client.list_buckets()['Buckets']
            if b['Name'] in bucket_targets
        ]

    # Each bucket gets its own worker
    with ThreadPoolExecutor() as tpe:

        jobs = []
        for bucket in buckets:
            jobs.append(tpe.submit(
                purge,
                bucket, max_days, access_key, secret_key, endpoint,
                list_then_delete=list_then_delete, dry_run=dry_run
            ))

        # I don't really care about the results
        for job in as_completed(jobs):
            try:
                job.result()
            except Exception as e:
                log.exception(e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', default=90, type=int, help='days to keep')
    parser.add_argument('--access-key', required=True, type=str, help='s3 access key')
    parser.add_argument('--secret-key', required=True, type=str, help='s3 secret key')
    parser.add_argument('--endpoint', required=True, type=str, help='endpoint url')
    parser.add_argument('--target-method', choices=['prefix', 'buckets'], default='prefix', help='method of bucket selection')
    parser.add_argument('--prefix', type=str, help='bucket prefix to target')
    parser.add_argument('-b', '--buckets', nargs='*', help='explicit buckets to target')
    parser.add_argument('--delete-method', default='list_then_delete', choices=['list_then_delete', 'delete_every_page'], help='deletion method')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='debug logging')
    parser.add_argument('--dry-run', action='store_true', default=False, help='dry run mode, do not delete')
    args = parser.parse_args()
    if args.debug:
        log.setLevel(logging.DEBUG)
        logging.getLogger('botocore').setLevel(logging.DEBUG)
    do(args)


if __name__ == '__main__':
    main()
