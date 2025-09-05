import boto3
import json
from datetime import datetime, timedelta
import logging
from typing import List, Dict
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class S3LifecycleMonitor:

    def __init__(self, aws_profile=None, region='us-west-2'):
        """
        Initialize AWS clients for monitoring
        """
        session = boto3.Session(
            profile_name=aws_profile) if aws_profile else boto3.Session()
        self.s3_client = session.client('s3', region_name=region)
        self.cloudtrail_client = session.client('cloudtrail',
                                                region_name=region)
        self.cloudwatch_client = session.client('cloudwatch',
                                                region_name=region)
        self.sns_client = session.client('sns', region_name=region)

    def get_objects_to_be_deleted(self, bucket_name: str) -> List[Dict]:
        """
        Get all objects currently tagged for deletion
        """
        objects_to_delete = []

        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name)

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Check if object has the deletion tag
                        try:
                            tags_response = self.s3_client.get_object_tagging(
                                Bucket=bucket_name,
                                Key=obj['Key']
                            )

                            tags = {tag['Key']: tag['Value'] for tag in
                                    tags_response.get('TagSet', [])}

                            if tags.get('TO_BE_DELETED') == 'true':
                                # Calculate deletion date (object creation + 2 days)
                                deletion_date = obj[
                                                    'LastModified'] + timedelta(
                                    days=2)

                                objects_to_delete.append({
                                    'Key': obj['Key'],
                                    'Size': obj['Size'],
                                    'LastModified': obj['LastModified'],
                                    'DeletionDate': deletion_date,
                                    'DaysUntilDeletion': (
                                                deletion_date - datetime.now(
                                            deletion_date.tzinfo)).days,
                                    'Tags': tags
                                })

                        except Exception as e:
                            logger.warning(
                                f"Could not get tags for {obj['Key']}: {e}")

        except Exception as e:
            logger.error(f"Error listing objects: {e}")

        return objects_to_delete

    def get_recent_deletions(self, bucket_name: str, hours_back: int = 24) -> \
    List[Dict]:
        """
        Get objects deleted by lifecycle policy in the last N hours using CloudTrail
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)

        deletions = []

        try:
            # Query CloudTrail for S3 deletion events
            response = self.cloudtrail_client.lookup_events(
                LookupAttributes=[
                    {
                        'AttributeKey': 'EventName',
                        'AttributeValue': 'DeleteObject'
                    }
                ],
                StartTime=start_time,
                EndTime=end_time
            )

            for event in response.get('Events', []):
                # Parse CloudTrail event
                cloud_trail_event = json.loads(event['CloudTrailEvent'])

                # Check if this is from our bucket and potentially from lifecycle
                if (cloud_trail_event.get(
                        'eventSource') == 's3.amazonaws.com' and
                        bucket_name in str(
                            cloud_trail_event.get('resources', []))):

                    # Extract object information
                    resources = cloud_trail_event.get('resources', [])
                    for resource in resources:
                        if resource.get('resourceType') == 'AWS::S3::Object':
                            object_arn = resource.get('resourceName', '')
                            object_key = object_arn.split('/')[
                                -1] if '/' in object_arn else object_arn

                            deletions.append({
                                'ObjectKey': object_key,
                                'DeletionTime': event['EventTime'],
                                'UserIdentity': cloud_trail_event.get(
                                    'userIdentity', {}),
                                'SourceIPAddress': cloud_trail_event.get(
                                    'sourceIPAddress'),
                                'EventId': event['EventId']
                            })

        except Exception as e:
            logger.error(f"Error querying CloudTrail: {e}")

        return deletions

    def create_monitoring_report(self, bucket_name: str,
                                 output_file: str = None) -> Dict:
        """
        Generate comprehensive monitoring report
        """
        logger.info("Generating monitoring report...")

        # Get objects scheduled for deletion
        to_be_deleted = self.get_objects_to_be_deleted(bucket_name)

        # Get recent deletions
        recent_deletions = self.get_recent_deletions(bucket_name,
                                                     hours_back=24)

        # Calculate statistics
        total_objects_tagged = len(to_be_deleted)
        total_size_tagged = sum(obj['Size'] for obj in to_be_deleted)

        # Group by days until deletion
        deletion_schedule = defaultdict(list)
        for obj in to_be_deleted:
            days = max(0, obj['DaysUntilDeletion'])
            deletion_schedule[days].append(obj)

        # Create report
        report = {
            'generated_at': datetime.utcnow().isoformat(),
            'bucket_name': bucket_name,
            'summary': {
                'total_objects_tagged_for_deletion': total_objects_tagged,
                'total_size_tagged_mb': round(total_size_tagged / 1024 / 1024,
                                              2),
                'objects_deleted_last_24h': len(recent_deletions)
            },
            'deletion_schedule': {
                f'day_{day}': {
                    'count': len(objects),
                    'size_mb': round(
                        sum(obj['Size'] for obj in objects) / 1024 / 1024, 2),
                    'objects': [obj['Key'] for obj in objects[:10]]
                    # Show first 10
                }
                for day, objects in sorted(deletion_schedule.items())
            },
            'recent_deletions': recent_deletions,
            'objects_to_be_deleted': to_be_deleted
        }

        # Save to file if specified
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Report saved to {output_file}")

        # Print summary
        logger.info("=" * 60)
        logger.info("LIFECYCLE MONITORING REPORT")
        logger.info("=" * 60)
        logger.info(f"Bucket: {bucket_name}")
        logger.info(f"Objects tagged for deletion: {total_objects_tagged}")
        logger.info(
            f"Total size tagged: {report['summary']['total_size_tagged_mb']} MB")
        logger.info(f"Objects deleted in last 24h: {len(recent_deletions)}")

        if deletion_schedule:
            logger.info("\nDeletion Schedule:")
            for day in sorted(deletion_schedule.keys()):
                if day == 0:
                    logger.info(
                        f"  Today: {len(deletion_schedule[day])} objects")
                elif day == 1:
                    logger.info(
                        f"  Tomorrow: {len(deletion_schedule[day])} objects")
                else:
                    logger.info(
                        f"  In {day} days: {len(deletion_schedule[day])} objects")

        return report

    def setup_cloudwatch_alarm(self, bucket_name: str, sns_topic_arn: str,
                               threshold: int = 100) -> bool:
        """
        Set up CloudWatch alarm for monitoring deletion activity
        """
        try:
            alarm_name = f"S3-LifecycleDeletions-{bucket_name}"

            # Create custom metric alarm
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='ObjectsDeleted',
                Namespace='S3/Lifecycle',
                Period=86400,  # 24 hours
                Statistic='Sum',
                Threshold=threshold,
                ActionsEnabled=True,
                AlarmActions=[sns_topic_arn],
                AlarmDescription=f'Alert when more than {threshold} objects are deleted by lifecycle policy in {bucket_name}',
                Dimensions=[
                    {
                        'Name': 'BucketName',
                        'Value': bucket_name
                    }
                ],
                Unit='Count'
            )

            logger.info(
                f"CloudWatch alarm '{alarm_name}' created successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to create CloudWatch alarm: {e}")
            return False

    def send_alert(self, sns_topic_arn: str, subject: str,
                   message: str) -> bool:
        """
        Send alert via SNS
        """
        try:
            self.sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=message,
                Subject=subject
            )
            logger.info("Alert sent successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
            return False


def main():
    # Configuration
    BUCKET_NAME = "svodvideos"
    AWS_PROFILE = None  # TODO
    OUTPUT_FILE = f"lifecycle_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    SNS_TOPIC_ARN = "arn:aws:sns:us-west-2:123456789012:s3-lifecycle-alerts"  # Optional
    ALERT_THRESHOLD = 100  # Alert if more than 100 objects deleted per day

    # Create monitor
    monitor = S3LifecycleMonitor(aws_profile=AWS_PROFILE)

    # Generate monitoring report
    report = monitor.create_monitoring_report(BUCKET_NAME, OUTPUT_FILE)

    # Optional: Set up CloudWatch alarm
    if SNS_TOPIC_ARN:
        monitor.setup_cloudwatch_alarm(BUCKET_NAME, SNS_TOPIC_ARN,
                                       ALERT_THRESHOLD)

    # Optional: Send alert if too many objects are scheduled for deletion
    objects_today = sum(1 for obj in report['objects_to_be_deleted'] if
                        obj['DaysUntilDeletion'] <= 0)
    if objects_today > ALERT_THRESHOLD and SNS_TOPIC_ARN:
        alert_message = f"""
        ALERT: High number of objects scheduled for deletion

        Bucket: {BUCKET_NAME}
        Objects to be deleted today: {objects_today}
        Threshold: {ALERT_THRESHOLD}

        Please review the lifecycle policy and tagged objects.
        """

        monitor.send_alert(
            SNS_TOPIC_ARN,
            f"S3 Lifecycle Alert: {BUCKET_NAME}",
            alert_message
        )


if __name__ == "__main__":
    main()