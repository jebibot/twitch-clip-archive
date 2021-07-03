from twitchAPI.twitch import Twitch
from datetime import datetime, timezone
import boto3
from botocore.errorfactory import ClientError

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/'

twitch = Twitch()
twitch.authenticate_app([])

users = twitch.get_users(logins=[LOGIN])
broadcaster_id = users['data'][0]['id']
print('Broadcaster ID:', broadcaster_id)

cursor = None
i = 0

while True:
    clips = twitch.get_clips(broadcaster_id, '', '', started_at=datetime(YEAR, 1, 1, tzinfo=timezone.utc), ended_at=datetime(YEAR + 1, 1, 1, tzinfo=timezone.utc), first=100, after=cursor)

    if 'error' in clips:
        continue

    for clip in clips['data']:

        try:
            s3.head_object(Bucket='t-clips', Key='{}/{}.mp4'.format(clip['broadcaster_id'], clip['id']))
        except ClientError:
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageAttributes={
                    'id': {
                        'DataType': 'String',
                        'StringValue': clip['id'],
                    },
                    'broadcaster_id': {
                        'DataType': 'String',
                        'StringValue': clip['broadcaster_id'],
                    },
                    'thumbnail_url': {
                        'DataType': 'String',
                        'StringValue': clip['thumbnail_url'],
                    },
                    'game_id': {
                        'DataType': 'String',
                        'StringValue': clip['game_id'] or 'None',
                    },
                    'title': {
                        'DataType': 'String',
                        'StringValue': clip['title'],
                    },
                    'view_count': {
                        'DataType': 'String',
                        'StringValue': str(clip['view_count']),
                    },
                    'created_at': {
                        'DataType': 'String',
                        'StringValue': clip['created_at'],
                    }
                },
                MessageBody=clip['id']
            )

            print(i, clip['id'])
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                print(response)
        i += 1

    pagination = clips['pagination']
    try:
        cursor = pagination['cursor']
    except KeyError:
        break
