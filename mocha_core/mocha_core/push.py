#!/usr/bin/env python3
""" push.py
    Topics with msg_history PUSH. Their messages never go into the database
    or a sync: the translator sends each one at once to every client robot,
    one UDP datagram each, and the topic publisher there publishes it as it
    comes. This is for small messages that a newer one soon replaces, such
    as a robot's pose, so a lost one is not sent again. Nor is the order
    kept: UDP may reorder or repeat a datagram, and every one is published,
    so a subscriber that cares orders the messages by their own stamps.

    A datagram is MOCHA's 6-byte header (hash_comm.TsHeader) with the
    message's robot and topic, its time unused and zero, followed by the
    message as database_utils serializes it. It goes to the robot's
    IP-address at its push-port, by default one below its base-port. The
    base-port itself is no good: every ROS 2 process on a robot holds a UDP
    port of its own counting up from 7000, whatever its ROS domain, so the
    ports from a base-port of 7000 up are taken.
"""
import socket

import rclpy.time

import mocha_core.hash_comm as hash_comm

# The most a UDP datagram carries over IPv4, in bytes. A larger message is
# not sent.
MAX_DATAGRAM = 65507


def address(robot_configs, robot):
    """ Where a robot hears pushed messages: (IP-address, push-port), the
    port one below its base-port when its config names none. A host name is
    looked up here, once, not on every message; one that cannot be is kept,
    and each send to it fails and is logged. """
    config = robot_configs[robot]
    port = config.get("push-port", int(config["base-port"]) - 1)
    try:
        host = socket.gethostbyname(config["IP-address"])
    except OSError:
        host = config["IP-address"]
    return (host, int(port))


def datagram(robot_id, topic_id, payload):
    """ A message's datagram: its header, then its serialized payload """
    header = hash_comm.TsHeader.from_data(robot_id, topic_id, rclpy.time.Time())
    return header.bindigest() + payload


def parse(data):
    """ A datagram's (robot_id, topic_id, payload); None when it is too short
    to hold a header and a payload """
    n = hash_comm.TsHeader.HEADER_LENGTH
    if len(data) <= n:
        return None
    header = hash_comm.TsHeader.from_header(data[:n])
    return header.robot_id, header.topic_id, data[n:]
