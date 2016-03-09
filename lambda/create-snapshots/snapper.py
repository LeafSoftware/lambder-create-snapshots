import boto3
import logging
import pprint
from datetime import datetime

class Snapper:

  TAG_NAME = "LambderBackup"

  def __init__(self):
    self.ec2 = boto3.resource('ec2')
    logging.basicConfig()
    self.logger = logging.getLogger()

  def get_volumes_to_backup(self):
    filters = [{'Name':'tag-key', 'Values': [self.TAG_NAME]}]
    volumes = self.ec2.volumes.filter(Filters=filters)
    return volumes

  def backup_name(self, source_name):
    time_str = datetime.utcnow().isoformat() + 'Z'
    time_str = time_str.replace(':', '').replace('+', '')
    return source_name + '-' + time_str

  # Takes an snapshot or volume, returns the backup source
  def get_backup_source(self, resource):
    tags = filter(lambda x: x['Key'] == self.TAG_NAME, resource.tags)

    if len(tags) < 1:
      return None

    return tags[0]['Value']

  def create_snapshot(self, volume, description):
    return self.ec2.create_snapshot(
      VolumeId=volume,
      Description=description
    )

  def run(self):

    # create new backups
    volumes = self.get_volumes_to_backup()
    volume_count = len(list(volumes))

    self.logger.info("Found {0} volumes to be backed up".format(volume_count))

    for volume in volumes:
      source = self.get_backup_source(volume)
      self.logger.info('Backing up ' + source + ' ' + volume.id)

      name        = self.backup_name(source)
      description = "Backup of " + source

      snapshot = self.create_snapshot(volume.id, description)

      # add backup source tag to snapshot
      snapshot.create_tags(Tags=[{'Key': self.TAG_NAME, 'Value': source}])
