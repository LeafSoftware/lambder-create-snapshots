import logging
from snapper import Snapper

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# logger.setLevel(logging.DEBUG)

snapper = Snapper()

# This is the method that will be registered
# with Lambda and run on a schedule
def handler(event={}, context={}):
  if 'ping' in event:
    logger.info('pong')
    return {'message': 'pong'}

  logger.debug('starting run')
  snapper.run()

# If being called locally, just call handler
if __name__ == '__main__':
  import os
  import json
  import sys

  logging.basicConfig()
  event = {}

  # TODO if argv[1], read contents, parse into json
  if len(sys.argv) > 1:
    input_file = sys.argv[1]
    with open(input_file, 'r') as f:
      data = f.read()
    event = json.loads(data)

  result = handler(event)
  output = json.dumps(
    result,
    sort_keys=True,
    indent=4,
    separators=(',', ':')
  )
  logger.info(output)
