#!/usr/bin/env python3
""" PUSH topics (push.py): a datagram's round trip, the port, and two
robots on this computer, one pushing a topic to the other, which publishes
it without the database or a sync and drops what is not a pushed message. """

import os
import socket
import tempfile
import threading
import time
import unittest

import rclpy
import yaml
from colorama import Fore, Style
from geometry_msgs.msg import PointStamped
from nav_msgs.msg import Odometry
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node

import mocha_core.database_server as ds
import mocha_core.database_utils as du
import mocha_core.push as push
import mocha_core.topic_publisher as tp
import mocha_core.translator as tr


class test_push_datagram(unittest.TestCase):
    def test_a_datagram_gives_back_its_robot_topic_and_payload(self):
        self.assertEqual(push.parse(push.datagram(3, 7, b"abc")), (3, 7, b"abc"))

    def test_a_datagram_without_a_payload_is_none(self):
        self.assertIsNone(push.parse(b"\x01\x02\x03"))
        self.assertIsNone(push.parse(push.datagram(1, 1, b"")))

    def test_a_robot_hears_pushes_below_its_base_port(self):
        """ Unless its config names its push-port """
        configs = {"a": {"IP-address": "10.0.0.1", "base-port": "7000"},
                   "b": {"IP-address": "10.0.0.2", "base-port": "7000", "push-port": 9000}}
        self.assertEqual(push.address(configs, "a"), ("10.0.0.1", 6999))
        self.assertEqual(push.address(configs, "b"), ("10.0.0.2", 9000))


class DatabaseNode(Node):
    def __init__(self):
        # The translator and the topic publisher wait for this node's services
        super().__init__("mocha")


class test_push_between_robots(unittest.TestCase):
    """ Robot a pushes /pose (PUSH) to robot b; /odometry is an ordinary
    topic. b's topic publisher publishes a's pushes on /a/pose. """

    def setUp(self):
        print("\n", Fore.RED, 20 * "=", self._testMethodName, 20 * "=", Style.RESET_ALL)
        self.dir = tempfile.TemporaryDirectory()
        ports = []
        for _ in range(2):
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.bind(("127.0.0.1", 0))
                ports.append(s.getsockname()[1])
        self.robot_configs = {
            "a": {"node-type": "ground_robot", "IP-address": "127.0.0.1", "using-radio": "r",
                  "base-port": "7000", "push-port": ports[0], "clients": ["b"]},
            "b": {"node-type": "ground_robot", "IP-address": "127.0.0.1", "using-radio": "r",
                  "base-port": "7000", "push-port": ports[1], "clients": ["a"]},
        }
        self.topic_configs = {"ground_robot": [
            {"msg_topic": "/odometry", "msg_type": "nav_msgs/Odometry", "msg_priority": "NO_PRIORITY",
             "msg_history": "LAST_MESSAGE"},
            {"msg_topic": "/pose", "msg_type": "geometry_msgs/PointStamped", "msg_priority": "HIGH_PRIORITY",
             "msg_history": "PUSH"},
        ]}
        robots = os.path.join(self.dir.name, "robots.yaml")
        topics = os.path.join(self.dir.name, "topics.yaml")
        with open(robots, "w") as f:
            yaml.dump(self.robot_configs, f)
        with open(topics, "w") as f:
            yaml.dump(self.topic_configs, f)

        rclpy.init()
        self.db_node = DatabaseNode()
        self.dbs = ds.DatabaseServer(self.robot_configs, self.topic_configs, "a", self.db_node)
        self.executor = MultiThreadedExecutor(num_threads=8)
        self.executor.add_node(self.db_node)
        threading.Thread(target=self.executor.spin, daemon=True).start()
        self.translator = tr.TranslatorNode(this_robot="a", robot_configs=robots, topic_configs=topics)
        self.publisher = tp.TopicPublisherNode("b", robots, topics)
        self.listener = Node("listener")
        for n in (self.translator, self.publisher, self.listener):
            self.executor.add_node(n)
        self.heard = []
        self.listener.create_subscription(PointStamped, "/a/pose", self.heard.append, tp.QOS_PROFILE)
        self.pub = self.listener.create_publisher(PointStamped, "/pose", tr.QOS_PROFILE)
        time.sleep(1)

    def tearDown(self):
        self.publisher.shutdown()
        self.executor.shutdown()
        for n in (self.translator, self.publisher, self.listener, self.db_node):
            n.destroy_node()
        rclpy.shutdown()
        self.dir.cleanup()

    def wait_heard(self, count, timeout=5.0):
        end = time.time() + timeout
        while len(self.heard) < count and time.time() < end:
            time.sleep(0.05)

    def test_a_pushed_message_is_published_and_not_stored(self):
        for i in range(3):
            m = PointStamped()
            m.header.frame_id = "world"
            m.point.x = float(i)
            self.pub.publish(m)
            time.sleep(0.1)
        self.wait_heard(3)
        self.assertEqual([m.point.x for m in self.heard], [0.0, 1.0, 2.0])
        robot_id = du.get_robot_id_from_name(self.robot_configs, "a")
        stored = self.dbs.dbl.db.get(robot_id, {})
        self.assertEqual(sum(len(messages) for messages in stored.values()), 0)

    def test_a_datagram_that_is_no_pushed_message_is_dropped(self):
        """ Raw datagrams to b: a /pose whose payload does not deserialize,
        and a good Odometry claiming the ordinary /odometry, are dropped and
        published nowhere; a good /pose after them still comes """
        robot_id = du.get_robot_id_from_name(self.robot_configs, "a")
        to = push.address(self.robot_configs, "b")
        m = PointStamped()
        m.point.x = 7.0
        odometry = []
        self.listener.create_subscription(Odometry, "/a/odometry", odometry.append, tp.QOS_PROFILE)
        time.sleep(0.5)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.sendto(push.datagram(robot_id, 1, b"garbage"), to)
            s.sendto(push.datagram(robot_id, 0, du.serialize_ros_msg(Odometry())), to)
            s.sendto(push.datagram(robot_id, 1, du.serialize_ros_msg(m)), to)
        self.wait_heard(2, timeout=2.0)
        self.assertEqual([m.point.x for m in self.heard], [7.0])
        self.assertEqual(odometry, [])

    def test_a_push_port_taken_is_not_fatal(self):
        """ A second topic publisher for b finds b's push-port held by the
        first: it starts all the same, without hearing pushes """
        robots = os.path.join(self.dir.name, "robots.yaml")
        topics = os.path.join(self.dir.name, "topics.yaml")
        second = tp.TopicPublisherNode("b", robots, topics)
        try:
            self.assertIsNone(second.push_th)
        finally:
            second.shutdown()
            second.destroy_node()


if __name__ == "__main__":
    unittest.main()
