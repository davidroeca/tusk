# -*- coding: utf-8  -*-
import os

from unittest import TestCase

from tusk import Lock


class TuskTest(TestCase):
    def setUp(self):
        dsn = os.environ.get("DATABASE_URL")
        self.former = Lock("tusk", dsn or "postgres://localhost:5432/tusk")
        self.latter = Lock("tusk", dsn or "postgres://localhost:5432/tusk")

    def test_acquire_release(self):
        self.former.acquire()
        self.assertTrue(not self.latter.acquire(blocking=False))
        self.assertTrue(self.former.release())
        self.assertTrue(self.latter.acquire(blocking=False))

    def test_context_manager(self):
        with self.former:
            self.assertTrue(not self.latter.acquire(blocking=False))
        self.assertTrue(self.latter.acquire(blocking=False))

    def test_unlock_unheld_lock(self):
        self.assertFalse(self.former.release())
        self.former.acquire()
        self.assertTrue(self.former.release())
        self.assertFalse(self.former.release())

    def test_consecutive_acquire(self):
        # If a session already holds a given advisory lock,
        # additional requests by it will always succeed,
        # even if other sessions are awaiting the lock
        self.former.acquire()
        self.assertTrue(self.former.acquire())
        self.assertTrue(not self.latter.acquire(blocking=False))
        self.assertTrue(self.former.release())
        self.assertTrue(self.former.release())
        self.assertFalse(self.former.release())
        self.assertTrue(self.latter.acquire(blocking=False))

    def tearDown(self):
        self.former.release()
        self.latter.release()
