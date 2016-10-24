import boto3
import logging
import pprint
import os
import os.path
import json
from datetime import datetime

class Snapper:

  BACKUP_TAG = "LambderBackup"
  REPLICATE_TAG = "LambderReplicate"

  def __init__(self):
    self.ec2 = boto3.resource('ec2')
    logging.basicConfig()
    self.logger = logging.getLogger()

    # set location of config file
    script_dir = os.path.dirname(__file__)
    config_file = script_dir + '/config.json'

    # if there is a config file in place, load it in. if not, bail.
    if not os.path.isfile(config_file):
      self.logger.error(config_file + " does not exist")
      exit(1)
    else:
      config_data=open(config_file).read()
      config_json = json.loads(config_data)
      self.AWS_REGIONS=config_json['AWS_REGIONS']

  def get_volumes_to_backup(self):
    filters = [{'Name':'tag-key', 'Values': [self.BACKUP_TAG]}]
    volumes = self.ec2.volumes.filter(Filters=filters)
    return volumes

  def backup_name(self, source_name):
    time_str = datetime.utcnow().isoformat() + 'Z'
    time_str = time_str.replace(':', '').replace('+', '')
    return source_name + '-' + time_str

  def is_replicated(self, resource):
    tags = filter(lambda x: x['Key'] == self.REPLICATE_TAG, resource.tags)

    if len(tags) < 1:
      return False

    return True

  # Takes an snapshot or volume, returns the backup source
  def get_backup_source(self, resource):
    tags = filter(lambda x: x['Key'] == self.BACKUP_TAG, resource.tags)

    if len(tags) < 1:
      return None

    return tags[0]['Value']

  def create_snapshot(self, volume, description):
    return self.ec2.create_snapshot(
      VolumeId=volume,
      Description=description
    )

    # Takes an snapshot or volume, returns the backup source
  def get_backup_source(self, resource):
    tags = filter(lambda x: x['Key'] == self.BACKUP_TAG, resource.tags)

    if len(tags) < 1:
      return None

    return tags[0]['Value']

  def get_snapshots_to_delete(self, snapshots, max_to_keep=3):
    snapshots_to_delete = []

    if len(snapshots) >= max_to_keep:
      number_to_delete = len(snapshots) - max_to_keep
      snapshots_to_delete = snapshots[0:number_to_delete]

    return snapshots_to_delete

  # return a Dict() of {backupsource: list_of_snapshots}
  def get_snapshots_by_backup_source(self,ec2byregion):
    filters = [{'Name':'tag-key', 'Values': [self.BACKUP_TAG]}]
    snapshots = ec2byregion.snapshots.filter(Filters=filters)

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
    for region in self.AWS_REGIONS:
      self.logger.info("running in region " + region)
      ec2byregion = boto3.resource('ec2', region_name=region)
      pp = pprint.PrettyPrinter()
      snapshots_by_source = self.get_snapshots_by_backup_source(ec2byregion)

      self.logger.debug('snapshots_by_source: ' + pp.pformat(snapshots_by_source))

      for source in snapshots_by_source.keys():
        all_snapshots = snapshots_by_source[source]
        to_delete = self.get_snapshots_to_delete(all_snapshots)

        self.logger.debug('to_delete: ' + pp.pformat(to_delete))

        for condemned in to_delete:
          self.logger.info("deleting " + condemned.snapshot_id)
          condemned.delete()

  def run(self):
    # prune the old snapshots to make room for the new ones being created
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
      snapshot.create_tags(Tags=[{'Key': self.BACKUP_TAG, 'Value': source}])

      if self.is_replicated(volume):
        snapshot.create_tags(Tags=[{'Key': self.REPLICATE_TAG, 'Value': ''}])
