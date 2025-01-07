"""Generic runner.

Example config file:

publisher_config:
  # at least one of the following two needs to be provided. If both are present, expected_files will take precedence
  expected_files: /tmp/pytest-of-a001673/pytest-169/test_fake_publisher0/file?.bla
  output_files_log_regex: "Written output file : (.*.nc)"
  publisher_settings:
    nameservers: false
    port: 1979
  static_metadata:
    sensor: thermometer
  topic: /hi/there
script:
  command: /tmp/pytest-of-a001673/pytest-169/test_fake_publisher0/myscript_bla.sh
  workers: 4
subscriber_config:
  addresses:
  - ipc://bla
  nameserver: false


"""
import argparse
import logging
import logging.config
import os
import re
from contextlib import closing, suppress
from functools import partial
from glob import glob
from multiprocessing.pool import ThreadPool
from subprocess import PIPE, Popen

import yaml
from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from posttroll.subscriber import create_subscriber_from_dict_config

logger = logging.getLogger("pytroll-runner")


def main(args=None):
    """Main script."""
    parsed_args = parse_args(args=args)
    setup_logging(parsed_args.log_config)

    logger.info("Start generic pytroll-runner")
    return run_and_publish(parsed_args.config_file, parsed_args.message_file)


def setup_logging(config_file):
    """Setup the logging from a log config yaml file."""
    if config_file is not None:
        with open(config_file) as fd:
            log_dict = yaml.safe_load(fd.read())
            logging.config.dictConfig(log_dict)
            return


def parse_args(args=None):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser("Pytroll Runner",
                                     description="Automate third party software in a pytroll environment")
    parser.add_argument("config_file",
                        help="The configuration file to run on.")
    parser.add_argument("-l", "--log_config",
                        help="The log configuration yaml file.",
                        default=None)
    parser.add_argument("-m", "--message-file",
                        help="A file containing messages to process.",
                        default=None)
    return parser.parse_args(args)


def run_and_publish(config_file, message_file=None):
    """Run the command and publish the expected files."""
    command_to_call, subscriber_config, publisher_config = read_config(config_file)
    with suppress(KeyError):
        preexisting_files = check_existing_files(publisher_config)

    with closing(create_publisher_from_dict_config(publisher_config["publisher_settings"])) as pub:
        pub.start()
        if message_file is None:
            gen = run_from_new_subscriber(command_to_call, subscriber_config)
        else:
            gen = run_from_message_file(command_to_call, message_file)
        for log_output, mda in gen:
            try:
                message = generate_message_from_log_output(publisher_config, mda, log_output)
            except KeyError:
                message = generate_message_from_expected_files(publisher_config, mda, preexisting_files)
                preexisting_files = check_existing_files(publisher_config)
            logger.debug(f"Sending message = {message}")
            pub.send(str(message))


def run_from_message_file(command_to_call, message_file):
    """Run the command on message file."""
    with open(message_file) as fd:
        messages = (Message(rawstr=line) for line in fd if line)
        yield from run_on_messages(command_to_call, messages)


def check_existing_files(publisher_config):
    """Check for previously generated files."""
    filepattern = publisher_config["expected_files"]
    return set(glob(filepattern))


def read_config(config_file):
    """Read the configuration file."""
    with open(config_file) as fd:
        config = yaml.safe_load(fd.read())
    return validate_config(config)


def validate_config(config):
    """Validate the configuration file."""
    publisher_config = config["publisher_config"]
    if "output_files_log_regex" not in publisher_config and "expected_files" not in publisher_config:
        raise KeyError("Missing ways to identify output files. "
                       "Either provide 'expected_files' or "
                       "'output_files_log_regex' in the config file.")

    subscriber_config = config["subscriber_config"]
    logger.debug("Subscriber config settings: ")
    for item, val in subscriber_config.items():
        logger.debug(f"{item} = {str(val)}")

    return config["script"], subscriber_config, publisher_config


def run_from_new_subscriber(command, subscriber_settings):
    """Run the command with files gotten from a new subscriber."""
    logger.debug("Run from new subscriber...")
    with closing(create_subscriber_from_dict_config(subscriber_settings)) as sub:
        yield from run_on_messages(command, sub.recv())


def run_on_messages(command, messages):
    """Run the command on files from messages."""
    try:
        num_workers = command.get("workers", 1)
    except AttributeError:
        num_workers = 1
    pool = ThreadPool(num_workers)
    run_command_on_message = partial(run_on_single_message, command)

    yield from pool.imap_unordered(run_command_on_message, select_messages(messages))


def select_messages(messages):
    """Select only valid messages."""
    accepted_message_types = ["file", "dataset"]
    for message in messages:
        if message.type not in accepted_message_types:
            continue
        yield message


def run_on_single_message(command, message):
    """Run the command on files from message."""
    try:  # file
        files = [message.data["uri"]]
    except KeyError:  # dataset
        files = []
        files.extend(info["uri"] for info in message.data["dataset"])
    return run_on_files(command, files), message.data


def run_on_files(command, files):
    """Run the command of files."""
    if not files:
        return
    logger.info(f"Start running command {command} on files {files}")
    try:
        command_to_call = command["command"]
    except TypeError:
        command_to_call = command
    process = Popen([*os.fspath(command_to_call).split(), *files], stdout=PIPE)  # noqa: S603
    out, _ = process.communicate()
    logger.debug(f"After having run the script: {out}")
    return out


def generate_message_from_log_output(publisher_config, mda, log_output):
    """Generate message for the filenames present in the log output."""
    logger.debug(f"type(log_output) = {type(log_output)}")
    logger.debug(log_output)
    if isinstance(log_output, bytes):
        log_output = log_output.decode('utf-8')

    logger.debug(str(publisher_config["output_files_log_regex"]))
    new_files = re.findall(publisher_config["output_files_log_regex"], log_output)
    logger.debug(f"Output files identified = {new_files}")
    if len(new_files) == 0:
        logger.warning("No output files to publish identified!")
    message = generate_message_from_new_files(publisher_config, new_files, mda)
    return message


def generate_message_from_expected_files(pub_config, extra_metadata=None, preexisting_files=None):
    """Generate a message containing the expected files."""
    new_files = find_new_files(pub_config, preexisting_files or set())

    return generate_message_from_new_files(pub_config, new_files, extra_metadata)


def generate_message_from_new_files(pub_config, new_files, extra_metadata):
    """Generate a message containing the new files."""
    metadata = populate_metadata(extra_metadata, pub_config.get("static_metadata", {}))
    dataset = []
    for filepath in sorted(new_files):
        filename = os.path.basename(filepath)
        dataset.append(dict(uid=filename, uri=filepath))
    if len(new_files) == 1:
        metadata.update(dataset[0])
        message_type = "file"
    else:
        metadata["dataset"] = dataset
        message_type = "dataset"
    return Message(pub_config["topic"], message_type, metadata)


def find_new_files(pub_config, preexisting_files):
    """Find new files matching the file pattern."""
    return check_existing_files(pub_config) - preexisting_files


def populate_metadata(extra_metadata, static_metadata):
    """Populate the metadata."""
    metadata = {}
    if extra_metadata is not None:
        metadata.update(extra_metadata)
        metadata.pop("uri", None)
        metadata.pop("uid", None)
        metadata.pop("dataset", None)
    metadata.update(static_metadata)
    return metadata
