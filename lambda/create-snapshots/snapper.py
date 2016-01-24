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

  def get_snapshots_to_delete(self, snapshots, max_to_keep=3):
    snapshots_to_delete = []

    if len(snapshots) >= max_to_keep:
      # remove one extra to make room for the next snapshot
      number_to_delete = len(snapshots) - max_to_keep + 1
      snapshots_to_delete = snapshots[0:number_to_delete]

    return snapshots_to_delete

  # return a Dict() of {backupsource: list_of_snapshots}
  def get_snapshots_by_backup_source(self):
    filters = [{'Name':'tag-key', 'Values': [self.TAG_NAME]}]
    snapshots = self.ec2.snapshots.filter(Filters=filters)

    results = {}
    for snapshot in snapshots:
      tag = self.get_backup_source(snapshot)
      if tag in results:
        results[tag].append(snapshot)
      else:
        results[tag] = [snapshot]

    for key in results.keys():
      results[key] = sorted(results[key], key=lambda x: x.start_time)

    return results

  def prune(self):
    pp = pprint.PrettyPrinter()
    snapshots_by_source = self.get_snapshots_by_backup_source()

    self.logger.debug('snapshots_by_source: ' + pp.pformat(snapshots_by_source))

    for source in snapshots_by_source.keys():
      all_snapshots = snapshots_by_source[source]
      to_delete = self.get_snapshots_to_delete(all_snapshots)

      self.logger.debug('to_delete: ' + pp.pformat(to_delete))

      for condemned in to_delete:
        self.logger.info("deleting " + condemned.snapshot_id)
        condemned.delete()

  def run(self):
    # prune old backups if needed
    self.prune()

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
      snapshot.create_tags(Tags=[{'Key': 'LambderBackup', 'Value': source}])
