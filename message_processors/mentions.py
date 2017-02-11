import json
import subprocess
import argparse
import logging
import re

import sqlalchemy.sql
import sqlalchemy_bonus

import unichat

logger = logging.getLogger(__name__)

mention_pattern = re.compile(r'@(?P<name>\w+)')

def process_message(message, processes, db_session):
  logger.debug('processing {}'.format(message))

  if message.speaker_name == 'Salazar':
    logger.debug('message is from Salazar; doing nothing')
    return

  for (command, value) in [('enable', 1), ('disable', 0)]:
    if message.content.strip() == '@salazar mentions '+command:
      logger.info(command+' mentions for '+message.speaker_name)
      person = db_session.query(unichat.Person).get(message.speaker_name)
      person.opted_into_snakechat_mention_notifications = value
      db_session.commit()
      outgoing_message = unichat.Message(
        time=message.time,
        thread_name=message.thread_name,
        speaker_name='Salazar',
        content='{}d mentions for {}'.format(command, message.speaker_name))
      logger.info('sending message: '+str(outgoing_message))
      processes['facebook'].stdin.write((json.dumps(outgoing_message.to_json_object()).strip()+'\n').encode())
      processes['facebook'].stdin.flush()

  db_session.commit()

  # if message.thread_name != 'SnakeChat':
  #   logger.debug('not in SnakeChat; doing nothing')
  #   return

  mentioned_names = set(m.group('name') for m in mention_pattern.finditer(message.content))
  mentioned_people = set(
    p for p in db_session.query(unichat.Person)
    if p.opted_into_snakechat_mention_notifications
    and any(re.match(name+r'\b', p.name, flags=re.I) for name in mentioned_names))
  logger.debug('mentioned names: {}'.format(mentioned_names))
  logger.info('notifying people: {}'.format(mentioned_people))

  for person in mentioned_people:
    outgoing_message = unichat.Message(
      time=message.time,
      thread_name='Facebook: '+person.name,
      speaker_name='Salazar',
      content='You were mentioned in {}:\n{}:\n{}'.format(message.thread_name, message.speaker_name, message.content))
    logger.info('sending message: '+str(outgoing_message))
    if person.fb_id:
      processes['facebook'].stdin.write((json.dumps(outgoing_message.to_json_object()).strip()+'\n').encode())
      processes['facebook'].stdin.flush()
