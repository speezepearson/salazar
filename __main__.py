import os
import glob
import queue
import threading
import subprocess
import json
import argparse
import contextlib
import logging
import traceback

import sqlalchemy as sql

import unichat

logger = logging.getLogger(__name__)

def enqueue_messages(file, q):
  for line in iter(file.readline, b''):
    q.put(unichat.Message.from_json_object(json.loads(line.decode())))

def load_process_message(path):
  vars_ = {}
  exec(open(path).read(), vars_, vars_)
  return vars_['process_message']

parser = argparse.ArgumentParser()
parser.add_argument('--verbose', action='store_true')
parser.add_argument('--message_processor_dir', default=os.path.join(os.path.dirname(__file__), 'message_processors'))
parser.add_argument('database')
args = parser.parse_args()

if args.verbose:
  logging.basicConfig(level=logging.DEBUG)

engine = sql.create_engine('sqlite:///'+args.database)
unichat.RelationBase.metadata.create_all(engine)
make_session = sql.orm.scoped_session(sql.orm.sessionmaker(bind=engine))

processes = {service_name: subprocess.Popen(
              ['python', '-m', 'unichat.services.{service_name}'.format(service_name=service_name), args.database],
              stdin=subprocess.PIPE,
              stdout=subprocess.PIPE,
              )
                for service_name in ['slack', 'facebook']}

with contextlib.ExitStack() as exit_stack:
  for process in processes.values():
    exit_stack.enter_context(process)

  q = queue.Queue(maxsize=1)
  for process in processes.values():
    threading.Thread(target=enqueue_messages, args=[process.stdout, q]).start()

  while True:
    message = q.get()
    logger.info('main loop got message: {}'.format(message))

    processor_path_glob = os.path.join(args.message_processor_dir, '*.py')
    logger.debug('looking for Python files in {!r}'.format(processor_path_glob))
    processor_paths = glob.glob(os.path.join(args.message_processor_dir, '*.py'))
    logger.debug('found {}'.format(processor_paths))
    for path in processor_paths:
      logger.info(' (passing it into {})'.format(os.path.basename(path)))
      try:
        load_process_message(path)(message=message, processes=processes, db_session=make_session())
      except Exception as e:
        traceback.print_exc()
