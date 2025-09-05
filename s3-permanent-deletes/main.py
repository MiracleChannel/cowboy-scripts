"""
Dirty but optimized script that automates the process of tagging files in an
S3 bucket based on a CSV file values.

It reads the CSV file, generates a list of regex patterns based on the
variable columns and then tags the matching S3 objects with a specific
tag key and value.

The script uses multi-threading to speed up the process and handle
errors.

Requires:
- Python 3.12
- boto3
- pandas

WARNINGS:
- Hardcoded for mp4 files only
- Hardcoded for TvShow prefixes only

"""

import boto3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from botocore.exceptions import ClientError
from typing import List, Tuple
import time
import re
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# noinspection LongLine,IncorrectFormatting
class S3RegexTagger:
    """Class for tagging S3 objects based on regex patterns"""

    def __init__(self, aws_profile=None, aws_region='us-west-2'):
        """
        Initialize S3 client

        Args:
            aws_profile: AWS profile name (optional)
            aws_region: AWS region
        """
        session = boto3.Session(
            profile_name=aws_profile) if aws_profile else boto3.Session()
        self.s3_client = session.client('s3', region_name=aws_region)

    def list_matching_objects(self, bucket_name: str, prefix: str,
                              pattern: str) -> List[str]:
        """
        List all objects matching the regex pattern with given prefix

        Args:
            bucket_name: S3 bucket name
            prefix: S3 prefix to narrow down search
            pattern: Regex pattern to match objects

        Returns:
            List of matching object keys
        """
        matching_objects = []
        compiled_pattern = re.compile(pattern)

        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if compiled_pattern.match(key):
                            matching_objects.append(key)

        except ClientError as e:
            logger.error(f"Error listing objects with prefix '{prefix}': {e}")

        return matching_objects

    def tag_single_object(self, bucket_name: str, object_key: str, tag_key: str, tag_value: str) -> Tuple[str, bool, str]:
        """
        Tag a single S3 object with tag_key: tag_value

        Args:
            bucket_name: S3 bucket name
            object_key: S3 object key
            tag_key: Tag key to add to object
            tag_value: Tag value to add to object
        Returns:
            Tuple of (object_key, success, message)
        """

        try:
            # Get existing tags
            try:
                response = self.s3_client.get_object_tagging(
                    Bucket=bucket_name, Key=object_key)
                existing_tags = response.get('TagSet', [])
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    return object_key, False, "Object not found"
                existing_tags = []

            # Check if tag already exists
            tag_exists = any(
                tag['Key'] == tag_key for tag in existing_tags)

            if tag_exists:
                return object_key, True, "Tag already exists"

            # Add the new tag
            new_tags = existing_tags + [
                {'Key': tag_key, 'Value': tag_value}]

            # Apply tags
            self.s3_client.put_object_tagging(
                Bucket=bucket_name,
                Key=object_key,
                Tagging={'TagSet': new_tags}
            )

            return object_key, True, "Successfully tagged"

        except ClientError as e:
            error_msg = f"AWS Error: {e.response['Error']['Code']} - {e.response['Error']['Message']}"
            return object_key, False, error_msg
        except Exception as e:
            return object_key, False, f"Unexpected error: {str(e)}"

    def tag_objects_by_pattern(self, csv_file_path: str, bucket_name: str,
                               object_prefix: str, object_filename: str,
                               tag_key: str, tag_value: str,
                               max_workers: int = 50, batch_size: int = 100,
                               max_list_workers: int = 10):
        """
        Read CSV and tag S3 objects matching pattern {location}/{filename}*.mp4

        Args:
            csv_file_path: Path to CSV file
            bucket_name: S3 bucket name
            object_prefix: Column name for file location
            object_filename: Column name for base filename
            tag_key: Tag key of objects to be deleted
            tag_value: Tag value of objects to be deleted
            max_workers: Number of concurrent threads for tagging
            batch_size: Batch size for progress reporting
            max_list_workers: Number of concurrent threads for listing objects
        """
        start_time = time.time()

        # Read CSV
        logger.info(f"Reading CSV file: {csv_file_path}")
        try:
            df = pd.read_csv(csv_file_path)
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            return

        # Validate columns exist
        if object_prefix not in df.columns or object_filename not in df.columns:
            logger.error(
                f"Columns '{object_prefix}' or '{object_filename}' not found in CSV")
            logger.info(f"Available columns: {list(df.columns)}")
            return

        # Remove rows with NaN values
        df_clean = df.dropna(subset=[object_prefix, object_filename])

        logger.info(
            f"Processing {len(df_clean)} CSV rows to find matching MP4 files")

        # Group by location to optimize S3 listing calls
        location_groups = defaultdict(list)
        for _, row in df_clean.iterrows():
            location = str(row[object_prefix]).strip('/')
            filename = str(row[object_filename])
            location_groups[location].append(filename)

        logger.info(f"Found {len(location_groups)} unique locations")

        # Phase 1: Find all matching objects
        logger.info("Phase 1: Finding matching objects in S3...")
        all_matching_objects = set()

        def find_objects_for_location(location_data):
            """Find all matching objects for a given location"""

            print(location_data)
            location_, filenames = location_data
            location_ = f"TvShows/{location_}/"
            location_matches = []

            # Create regex pattern for all filenames in this location
            # Pattern: ^location/(filename1.*\.mp4|filename2.*\.mp4|...)$
            escaped_filenames = [re.escape(fn) for fn in filenames]
            filename_patterns = '|'.join(
                [f"{fn}.*\\.mp4" for fn in escaped_filenames])
            full_pattern = f"^{re.escape(location_)}({filename_patterns})$"

            try:
                matches_ = self.list_matching_objects(bucket_name, location_, full_pattern)
                location_matches.extend(matches_)
                logger.info(
                    f"Location '{location_}': found {len(matches_)} matching objects using pattern {full_pattern}")
            except Exception as err:
                logger.error(f"Error processing location '{location_}': {err}")

            return location_matches

        # Use threading for listing operations
        with ThreadPoolExecutor(max_workers=max_list_workers) as executor:
            future_to_location = {
                executor.submit(find_objects_for_location, item): item[0]
                for item in location_groups.items()
            }

            for future in as_completed(future_to_location):
                matches = future.result()
                all_matching_objects.update(matches)

        matching_objects_list = list(all_matching_objects)
        logger.info(
            f"Phase 1 complete: Found {len(matching_objects_list)} total matching objects")

        if not matching_objects_list:
            logger.warning(
                "No matching objects found. Please verify your CSV data and S3 bucket contents.")
            return

        # Phase 2: Tag all matching objects
        logger.info("Phase 2: Tagging matching objects...")
        successful = 0
        failed = 0
        already_tagged = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tagging tasks
            future_to_key = {
                executor.submit(self.tag_single_object, bucket_name, key,
                                tag_key, tag_value): key
                for key in matching_objects_list
            }

            # Process completed tasks
            for i, future in enumerate(as_completed(future_to_key), 1):
                object_key, success, message = future.result()

                if success:
                    if "already exists" in message:
                        already_tagged += 1
                    else:
                        successful += 1
                else:
                    failed += 1
                    logger.warning(f"Failed to tag {object_key}: {message}")

                # Progress reporting
                if i % batch_size == 0 or i == len(matching_objects_list):
                    logger.info(
                        f"Tagging progress: {i}/{len(matching_objects_list)} objects processed")

        # Summary
        elapsed_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info("TAGGING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"CSV rows processed: {len(df_clean)}")
        logger.info(f"Unique locations: {len(location_groups)}")
        logger.info(f"Matching objects found: {len(matching_objects_list)}")
        logger.info(f"Successfully tagged: {successful}")
        logger.info(f"Already tagged: {already_tagged}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Total time: {elapsed_time:.2f} seconds")
        logger.info(
            f"Rate: {len(matching_objects_list) / elapsed_time:.2f} objects/second")

        # Show sample matches
        if matching_objects_list:
            logger.info("\nSample matching objects:")
            for obj in matching_objects_list[:5]:
                logger.info(f"  - {obj}")
            if len(matching_objects_list) > 5:
                logger.info(f"  ... and {len(matching_objects_list) - 5} more")


# noinspection PyPep8Naming,PyMissingOrEmptyDocstring,TodoComment
def main():
    # Configuration
    # CSV_FILE_PATH = "delete_data/Videos-HardDeletes.csv"
    CSV_FILE_PATH = "delete_data/Videos-Test.csv"  # TODO
    OBJECT_PREFIX_COLUMN = "location_folder"
    OBJECT_FILENAME = "location_file"

    AWS_PROFILE = None  # TODO
    AWS_REGION = "us-west-2"
    BUCKET_NAME = "svodvideos"
    TAG_KEY = "PERMANENT_DELETE"
    TAG_VALUE = "CONFIRMED"

    MAX_WORKERS = 50
    MAX_LIST_WORKERS = 10

    tagger = S3RegexTagger(aws_profile=AWS_PROFILE, aws_region=AWS_REGION)
    tagger.tag_objects_by_pattern(
        csv_file_path=CSV_FILE_PATH,
        bucket_name=BUCKET_NAME,
        object_prefix=OBJECT_PREFIX_COLUMN,
        object_filename=OBJECT_FILENAME,
        tag_key=TAG_KEY,
        tag_value=TAG_VALUE,
        max_workers=MAX_WORKERS,
        max_list_workers=MAX_LIST_WORKERS
    )


if __name__ == "__main__":
    main()
