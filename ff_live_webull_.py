###############################################################
# hft_interactive_news_client.py - Modernized UI/UX
# Upgraded styling with dark background
# OPTIMIZED FOR 500 MAX CHARTS - FULLY RESPONSIVE
###############################################################

import asyncio
from asyncio import Queue, QueueEmpty
import websockets
import orjson
from datetime import datetime, timedelta
import numpy as np
import logging
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
import aiohttp
import pytz
import re
from typing import Dict, Any, Tuple, List
import heapq
import time
import math
import pygame
import os
import argparse
import colorsys
import random
from dataclasses import dataclass
from functools import lru_cache
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
from multiprocessing import shared_memory
import threading
import sys
from qasync import QEventLoop

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QTreeWidget, QTreeWidgetItem, QTabWidget,
    QMessageBox, QSplitter, QLineEdit, QListWidget, QSizePolicy,
    QFrame, QScrollArea, QGridLayout, QComboBox, QCheckBox, QSpacerItem,
    QToolButton, QHeaderView, QGraphicsDropShadowEffect, QStyleFactory,
    QSpinBox, QDoubleSpinBox, QAbstractItemView, QMenu,
    QListWidgetItem, QGraphicsBlurEffect
)
from PyQt6.QtCore import (
    Qt, QTimer, pyqtSignal, pyqtSlot, QRunnable, QThreadPool, QEvent, QObject,
    QSize, QPropertyAnimation, QRect, QEasingCurve, QPoint, QAbstractAnimation,
    QSequentialAnimationGroup, QParallelAnimationGroup, pyqtProperty
)
from PyQt6.QtGui import (
    QColor, QPixmap, QIcon, QFont, QPalette, QLinearGradient, QGradient,
    QBrush, QPainter, QFontDatabase, QCursor, QPen, QRadialGradient,
    QFontMetrics, QKeySequence, QShortcut, QAction, QContextMenuEvent
)

import pyqtgraph as pg

import pynput
from pynput import mouse, keyboard

QApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)

mouse_controller = mouse.Controller()
keyboard_controller = keyboard.Controller()

ALLOWED_RIC_SUFFIXES = {'.O', '.N', '.A'}

# Clock display modes
CLOCK_MODE_WORLD = "world"
CLOCK_MODE_UTC = "utc"
CLOCK_MODE_NY_12H = "ny12"
CLOCK_MODE_NY_24H = "ny24"

# CONFIGURED FOR 500 MAX CHARTS
MAX_SYMBOLS = 1000  # Keep high for symbol tracking
MAX_CHART_LINES = 500  # Increased from 100 to 500
CHART_WARNING_THRESHOLD = 499  # Warn at 499 instead of 99

#config for 1 min window data points
SHORT_MAX_POINTS = 3000  # keep 3000 samples while 1‑min view is active

##################################################
# Profiling Decorator (runtime toggle)
##################################################
ENABLE_PROFILING = False

def profile_function(func):
    """Decorator to log execution time when profiling enabled."""

    async def async_wrapper(*args, **kwargs):
        if not ENABLE_PROFILING:
            return await func(*args, **kwargs)
        start_time = time.perf_counter()
        logging.debug(f"[PROFILE START] {func.__name__} args={args} kwargs={kwargs}")
        result = await func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed = (end_time - start_time) * 1000
        logging.debug(f"[PROFILE END] {func.__name__} took {elapsed:.2f} ms")
        return result

    def sync_wrapper(*args, **kwargs):
        if not ENABLE_PROFILING:
            return func(*args, **kwargs)
        start_time = time.perf_counter()
        logging.debug(f"[PROFILE START] {func.__name__} args={args} kwargs={kwargs}")
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed = (end_time - start_time) * 1000
        logging.debug(f"[PROFILE END] {func.__name__} took {elapsed:.2f} ms")
        return result

    return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

# Adjusted intervals for batching and throttling UI updates and processing:
BATCH_INTERVAL = 0.05
CHART_UPDATE_INTERVAL = 0.05  # For smoother chart updates

@profile_function
def calculate_quantity(price, investment_amount, slippage_percentage):
    """Given a price, an investment amount, and a slippage %, compute the share quantity."""
    MIN_PRICE_THRESHOLD = 0.01  # Define a minimum sensible price, e.g., 1 cent.
    try:
        # Check against the threshold instead of just <= 0
        if price is None or price < MIN_PRICE_THRESHOLD:
            logging.warning(f"Invalid or excessively low price ({price}) for quantity calculation")
            return None
        slippage_amount = price * (slippage_percentage / 100)
        max_price = price + slippage_amount

        # Safety check, though typically unreachable if MIN_PRICE_THRESHOLD is positive
        if max_price <= 0:
             return None

        quantity = investment_amount // max_price
        logging.info(f"Calculated quantity: {quantity}")
        return quantity
    except Exception as e:
        logging.exception("Error occurred in calculate_quantity")
        return None

###################################################
# Synchronous equivalents for external workflow
###################################################
@profile_function
def click_coordinates_sync(x, y):
    """Synchronous version to move & click. Contains necessary sleeps."""
    try:
        mouse_controller.position = (x, y)
        mouse_controller.click(mouse.Button.left)
        time.sleep(0.01)
        logging.info(f"Clicked coordinates: ({x}, {y})")
    except Exception as e:
        logging.exception("Error occurred in click_coordinates")

@profile_function
def typewrite_text_sync(text):
    """Synchronous version to type text. Contains necessary sleeps."""
    try:
        keyboard_controller.type(text)
        time.sleep(0.01)
        logging.info(f"Typewritten text: {text}")
    except Exception as e:
        logging.exception("Error occurred in typewrite_text")

@profile_function
def take_action_sync(symbol, quantity, mouse_button):
    """
    Synchronous version of take_action. We do not remove any sleeps,
    but we ensure it runs in a Worker thread, so the main loop
    remains free.
    """
    try:
        click_coordinates_sync(788, 20)
        time.sleep(0.05)
        click_coordinates_sync(788, 20)
        time.sleep(0.025)

        # Type symbol
        time.sleep(0.05)
        typewrite_text_sync(symbol)
        time.sleep(0.5)
        keyboard_controller.press(keyboard.Key.enter)
        time.sleep(0.1)
        keyboard_controller.release(keyboard.Key.enter)
        time.sleep(0.1)

        # Click next coordinate to focus quantity field
        click_coordinates_sync(1049, 114)
        time.sleep(.10)
        click_coordinates_sync(1049, 114)
        time.sleep(.1)

        # Type quantity
        typewrite_text_sync(str(quantity))
        time.sleep(0.1)

        if mouse_button == Qt.MouseButton.LeftButton:
            final_coords = (1227, 767)
        else:
            final_coords = (1227, 767)

        mouse_controller.position = final_coords

        logging.info(
            f"Action taken ({'Left' if mouse_button == Qt.MouseButton.LeftButton else 'Right'} Click): "
            f"Bought {quantity} shares of {symbol}"
        )

    except Exception as e:
        logging.exception("Error occurred in take_action")

POLYGON_API_KEY = "pjedsfYZgxvKKfcp0T1iLxO_S78tLqWj"
POLYGON_WS_URI = "wss://socket.polygon.io/stocks"
POLYGON_REST_URL = "https://api.polygon.io"
NEWS_WS_URI = "ws://localhost:8000/ws"
DEFAULT_CHART_HISTORY_SECONDS = 100000
SHORT_CHART_HISTORY_SECONDS = 60
DURATION = 50
TEXT_LABEL_PADDING_PX = 100  # Padding in pixels to keep price labels visible

# ----------------------------------------------------------------------
# LOGGING CONFIGURATION - More verbose with ms timestamps
# ----------------------------------------------------------------------
log_file = 'interactive_news_feed.log'
log_queue = multiprocessing.Queue(-1)
queue_handler = QueueHandler(log_queue)
file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
stream_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[queue_handler]
)

queue_listener = QueueListener(log_queue, file_handler, stream_handler)
queue_listener.start()
logging.info(f"Logging initiated. Log file: {os.path.abspath(log_file)}")

def configure_logging(verbose: bool = False):
    """Configure logging level at runtime."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.getLogger().setLevel(level)

@profile_function
def make_timestamp_naive_utc(timestamp):
    """Returns a naive (tzinfo=None) datetime in UTC."""
    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is not None:
            return timestamp.astimezone(pytz.UTC).replace(tzinfo=None)
        else:
            return timestamp
    elif isinstance(timestamp, str):
        try:
            parsed_timestamp = datetime.fromisoformat(timestamp)
            if parsed_timestamp.tzinfo is not None:
                return parsed_timestamp.astimezone(pytz.UTC).replace(tzinfo=None)
            else:
                return parsed_timestamp
        except ValueError:
            logging.warning(f"Unable to parse timestamp string: {timestamp}. Using current UTC time.")
            return datetime.utcnow()
    else:
        logging.warning(f"Unexpected timestamp type: {type(timestamp)}. Using current UTC time.")
        return datetime.utcnow()

#############################################################
# Multiprocessing helper functions
#############################################################
def process_price_batch_worker(price_updates, script_start_time):
    """Aggregate raw price updates and return per-symbol results."""
    valid = []
    for d in price_updates:
        if not isinstance(d, dict):
            continue
        sym = d.get('sym')
        price = d.get('p')
        volume = d.get('s')
        ts = d.get('t')
        if sym and price is not None and volume is not None and isinstance(ts, (int, float)):
            valid.append((sym, price, volume, ts))
    if not valid:
        return {}

    dtype = [('sym', 'U20'), ('p', 'f8'), ('s', 'f8'), ('t', 'i8')]
    arr = np.array(valid, dtype=dtype)
    unique_syms = np.unique(arr['sym'])
    results = {}
    for sym in unique_syms:
        mask = arr['sym'] == sym
        price = arr['p'][mask][-1]
        volume = arr['s'][mask].sum()
        raw_ts = arr['t'][mask][-1]
        timestamp = datetime.utcfromtimestamp(raw_ts / 1000).replace(tzinfo=pytz.UTC)
        timestamp = make_timestamp_naive_utc(timestamp)
        if timestamp < script_start_time:
            timestamp = script_start_time
        results[sym] = (float(price), float(volume), timestamp)
    return results


def process_news_batch_worker(news_items):
    """Filter out invalid news items for further processing."""
    return [item for item in news_items if isinstance(item, dict)]

################################################################
# Animated Accent Line Widget
################################################################
class AnimatedAccentLine(QWidget):
    def __init__(self, parent=None, color="#8b5cf6", orientation="horizontal"):
        super().__init__(parent)
        self.color = QColor(color)
        self.orientation = orientation
        self._position = 0
        
        if orientation == "horizontal":
            self.setFixedHeight(2)
            self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        else:
            self.setFixedWidth(2)
            self.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Expanding)
        
        # Animation
        self.animation = QPropertyAnimation(self, b"position")
        self.animation.setDuration(3000)
        self.animation.setStartValue(0)
        self.animation.setEndValue(100)
        self.animation.setLoopCount(-1)  # Infinite loop
        self.animation.start()
    
    @pyqtProperty(int)
    def position(self):
        return self._position
    
    @position.setter
    def position(self, value):
        self._position = value
        self.update()
    
    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        gradient = QLinearGradient()
        if self.orientation == "horizontal":
            gradient.setStart(0, 0)
            gradient.setFinalStop(self.width(), 0)
        else:
            gradient.setStart(0, 0)
            gradient.setFinalStop(0, self.height())
        
        # Create animated gradient
        pos = self._position / 100.0
        gradient.setColorAt(0, QColor(self.color.red(), self.color.green(), self.color.blue(), 0))
        gradient.setColorAt(max(0, pos - 0.1), QColor(self.color.red(), self.color.green(), self.color.blue(), 0))
        gradient.setColorAt(pos, self.color)
        gradient.setColorAt(min(1, pos + 0.1), QColor(self.color.red(), self.color.green(), self.color.blue(), 0))
        gradient.setColorAt(1, QColor(self.color.red(), self.color.green(), self.color.blue(), 0))
        
        painter.fillRect(self.rect(), gradient)

################################################################
# Event system for decoupling
################################################################
@dataclass(slots=True)
class Event:
    type: str
    data: Any

class EventBus(QWidget):
    event_occurred = pyqtSignal(object)
    __slots__ = ("listeners",)

    @profile_function
    def __init__(self):
        super().__init__()
        self.listeners = {}

    @profile_function
    def subscribe(self, event_type, callback):
        if event_type not in self.listeners:
            self.listeners[event_type] = []
        self.listeners[event_type].append(callback)

    @profile_function
    def publish(self, event):
        self.event_occurred.emit(event)

    @pyqtSlot(object)
    @profile_function
    def process_event(self, event):
        if event.type in self.listeners:
            for callback in self.listeners[event.type]:
                callback(event.data)

################################################################
# Worker for threaded tasks
################################################################
class WorkerSignals(QObject):
    finished = pyqtSignal(object)

class Worker(QRunnable):
    __slots__ = ("fn", "args", "kwargs", "signals")
    @profile_function
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    @profile_function
    def run(self):
        result = self.fn(*self.args, **self.kwargs)
        self.signals.finished.emit(result)

################################################################
# Non-blocking Audio
################################################################
class NonBlockingAudio:
    """Play beeps using pygame without blocking the main thread.

    Beeps normally play for ``duration`` milliseconds. When the number of active
    sounds exceeds ``fast_mode_threshold`` the duration is capped at
    ``fast_max_duration`` and the volume is reduced. When the number of active
    sounds exceeds ``backlog_threshold`` the volume of all playing sounds is
    gradually reduced to avoid overwhelming the output. When the total active
    sounds exceeds ``max_queue_size`` the oldest sound is faded out.
    """

    __slots__ = (
        "active_channels",
        "sounds",
        "backlog_check_interval",
        "backlog_threshold",
        "monitor_thread",
        "fast_mode_threshold",
        "fast_max_duration",
        "max_queue_size",
        "lock",
        "allow_polyphony",
    )

    @profile_function
    def __init__(self, backlog_check_interval=30, backlog_threshold=100,
                 fast_mode_threshold=10, fast_max_duration=50,
                 max_queue_size=500, allow_polyphony=True):
        if not pygame.mixer.get_init():
            try:
                pygame.mixer.init(frequency=22050, size=-16, channels=2, buffer=512)
            except pygame.error as exc:
                logging.error(f"Failed to initialize pygame mixer: {exc}")
        self.active_channels = deque()
        self.sounds = {}
        self.backlog_check_interval = backlog_check_interval
        self.backlog_threshold = backlog_threshold
        self.fast_mode_threshold = fast_mode_threshold
        self.fast_max_duration = fast_max_duration
        self.max_queue_size = max_queue_size
        self.lock = threading.Lock()
        self.allow_polyphony = allow_polyphony
        self.monitor_thread = threading.Thread(target=self._monitor_backlog, daemon=True)
        self.monitor_thread.start()

    @profile_function
    def _monitor_backlog(self):
        while True:
            time.sleep(self.backlog_check_interval)
            with self.lock:
                self._cleanup_channels()
                if len(self.active_channels) > self.backlog_threshold:
                    logging.warning(
                        "Audio backlog detected. Reducing volume of active sounds")
                    for ch in list(self.active_channels):
                        try:
                            vol = ch.get_volume()
                            ch.set_volume(max(0.1, vol * 0.5))
                        except Exception:
                            pass

    @profile_function
    def _get_sound(self, frequency):
        if frequency not in self.sounds:
            sample_rate = 22050
            duration = 0.1
            volume = 0.4

            attack = 0.01
            decay = 0.02
            sustain = 0.7
            release = 0.07

            total_duration = duration + release
            t = np.linspace(0, total_duration, int(sample_rate * total_duration), False)

            wave = np.sin(2 * np.pi * frequency * t)

            envelope = np.zeros_like(t)
            attack_samples = int(attack * sample_rate)
            decay_samples = int(decay * sample_rate)
            sustain_samples = int(duration * sample_rate) - attack_samples - decay_samples
            release_samples = int(release * sample_rate)

            idx = 0
            if attack_samples > 0:
                envelope[idx:idx + attack_samples] = np.linspace(0, 1, attack_samples)
                idx += attack_samples
            if decay_samples > 0:
                envelope[idx:idx + decay_samples] = np.linspace(1, sustain, decay_samples)
                idx += decay_samples
            if sustain_samples > 0:
                envelope[idx:idx + sustain_samples] = sustain
                idx += sustain_samples
            if release_samples > 0 and idx < len(envelope):
                remaining = min(release_samples, len(envelope) - idx)
                envelope[idx:idx + remaining] = np.linspace(sustain, 0, remaining)

            wave = wave * envelope

            wave = wave * volume
            wave_int = np.int16(wave * 32767)
            stereo = np.column_stack((wave_int, wave_int))
            try:
                self.sounds[frequency] = pygame.sndarray.make_sound(stereo)
            except Exception as exc:
                logging.error(f"Failed to create sound for {frequency}Hz: {exc}")
                raise
        return self.sounds[frequency]

    @profile_function
    def _cleanup_channels(self):
        self.active_channels = deque(
            ch for ch in self.active_channels if ch.get_busy()
        )

    @profile_function
    def play_sound(self, frequency, duration):
        with self.lock:
            self._cleanup_channels()
            if not self.allow_polyphony:
                if self.active_channels:
                    return
            if len(self.active_channels) >= self.max_queue_size:
                ch = self.active_channels.popleft()
                try:
                    ch.fadeout(100)
                except Exception:
                    pass
            if len(self.active_channels) > self.fast_mode_threshold:
                duration = min(duration, self.fast_max_duration)
                volume = 0.5
            else:
                volume = 1.0
            sound = self._get_sound(frequency)
            channel = sound.play(maxtime=int(duration))
            if channel:
                channel.set_volume(volume)
                self.active_channels.append(channel)

    @profile_function
    def clear_queue(self, preserve_frequency=None):
        with self.lock:
            for ch in list(self.active_channels):
                try:
                    ch.fadeout(100)
                except Exception:
                    pass
            self.active_channels.clear()

################################################################
# Lock-free buffers
################################################################
class LockFreeCircularBuffer:
    __slots__ = ("buffer", "lock")
    @profile_function
    def __init__(self, maxlen):
        self.buffer = deque(maxlen=maxlen)
        self.lock = threading.Lock()

    @profile_function
    def append(self, item):
        with self.lock:
            self.buffer.append(item)

    @profile_function
    def extend(self, items):
        with self.lock:
            self.buffer.extend(items)

    @profile_function
    def __len__(self):
        with self.lock:
            return len(self.buffer)

    @profile_function
    def __getitem__(self, index):
        with self.lock:
            return list(self.buffer)[index]

    @profile_function
    def __iter__(self):
        with self.lock:
            return iter(list(self.buffer))

class PriceCircularBuffer:
    __slots__ = ("buffer", "lock")
    @profile_function
    def __init__(self, maxlen):
        self.buffer = deque(maxlen=maxlen)
        self.lock = threading.Lock()

    @profile_function
    def set_maxlen(self, new_maxlen):
        with self.lock:
            items = list(self.buffer)[-new_maxlen:]
            self.buffer = deque(items, maxlen=new_maxlen)

    @profile_function
    def append(self, timestamp, gain):
        with self.lock:
            self.buffer.append((timestamp, gain))

    @profile_function
    def extend(self, items):
        with self.lock:
            self.buffer.extend(items)

    @profile_function
    def __iter__(self):
        with self.lock:
            return iter(list(self.buffer))

################################################################
# Top tracker
################################################################
class TopTracker:
    __slots__ = (
        "max_items",
        "top_heap",
        "bottom_heap",
        "symbol_scores",
        "lock",
    )
    @profile_function
    def __init__(self, max_items=MAX_CHART_LINES):
        self.max_items = max_items
        self.top_heap = []
        self.bottom_heap = []
        self.symbol_scores = {}
        self.lock = threading.Lock()

    @profile_function
    def update(self, symbol, score):
        with self.lock:
            if symbol in self.symbol_scores:
                if score != self.symbol_scores[symbol]:
                    self._remove(symbol)
                    self._add(symbol, score)
            else:
                self._add(symbol, score)

    @profile_function
    def _add(self, symbol, score):
        self.symbol_scores[symbol] = score
        if len(self.top_heap) < self.max_items:
            heapq.heappush(self.top_heap, (-score, symbol))
        else:
            if score > -self.top_heap[0][0]:
                moved_item = heapq.heapreplace(self.top_heap, (-score, symbol))
                heapq.heappush(self.bottom_heap, (-moved_item[0], moved_item[1]))
            else:
                heapq.heappush(self.bottom_heap, (score, symbol))

    @profile_function
    def _remove(self, symbol):
        score = self.symbol_scores.pop(symbol)
        if (-score, symbol) in self.top_heap:
            self.top_heap.remove((-score, symbol))
            heapq.heapify(self.top_heap)
            if self.bottom_heap:
                item = heapq.heappop(self.bottom_heap)
                heapq.heappush(self.top_heap, (-item[0], item[1]))
        elif (score, symbol) in self.bottom_heap:
            self.bottom_heap.remove((score, symbol))
            heapq.heapify(self.bottom_heap)

    @profile_function
    def get_top(self):
        with self.lock:
            return sorted([(-score, symbol) for score, symbol in self.top_heap], reverse=True)[:self.max_items]

    @profile_function
    def cleanup_tracker(self, last_update_times):
        now = np.datetime64('now')
        cutoff = now - np.timedelta64(3, 'm')
        with self.lock:
            for symbol in list(self.symbol_scores.keys()):
                if symbol in last_update_times and last_update_times[symbol] < cutoff:
                    self._remove(symbol)

################################################################
# Musical Alarm System
################################################################
class MusicalAlarmSystem:
    __slots__ = (
        "scales",
        "root_frequencies",
        "symbol_notes",
        "audio_player",
        "note_counts",
        "lock",
        "stop_event",
        "player_thread",
        "current_scale_type",
        "current_root_freq",
        "scale_change_time",
        "circle_of_fifths",
        "circle_index",
    )
    @profile_function
    def __init__(self):
        self.scales = {
            'major': [0, 2, 4, 5, 7, 9, 11],
            'natural_minor': [0, 2, 3, 5, 7, 8, 10],
            'harmonic_minor': [0, 2, 3, 5, 7, 8, 11],
            'melodic_minor': [0, 2, 3, 5, 7, 9, 11],
            'dorian': [0, 2, 3, 5, 7, 9, 10],
            'phrygian': [0, 1, 3, 5, 7, 8, 10],
            'lydian': [0, 2, 4, 6, 7, 9, 11],
            'mixolydian': [0, 2, 4, 5, 7, 9, 10],
            'locrian': [0, 1, 3, 5, 6, 8, 10],
            'whole_tone': [0, 2, 4, 6, 8, 10],
            'diminished': [0, 2, 3, 5, 6, 8, 9, 11],
            'augmented': [0, 3, 4, 7, 8, 11],
            'blues': [0, 3, 5, 6, 7, 10],
            'pentatonic_major': [0, 2, 4, 7, 9],
            'pentatonic_minor': [0, 3, 5, 7, 10],
        }
        self.root_frequencies = [220.0 * (2 ** (i / 12)) for i in range(12)]
        # Pre-compute the circle of fifths using indices relative to the
        # A-based frequency table.
        self.circle_of_fifths = [
            self.root_frequencies[i] for i in
            [0, 7, 2, 9, 4, 11, 6, 1, 8, 3, 10, 5]
        ]
        self.circle_index = 0
        self.current_root_freq = self.circle_of_fifths[self.circle_index]
        self.current_scale_type = random.choice(list(self.scales.keys()))
        self.scale_change_time = time.time()
        self.symbol_notes = {}
        self.audio_player = NonBlockingAudio(
            backlog_check_interval=1,
            max_queue_size=100,
            allow_polyphony=False
        )
        self.note_counts = defaultdict(int)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.player_thread = threading.Thread(target=self._arpeggio_loop,
                                              daemon=True)
        self.player_thread.start()

    @profile_function
    def _update_scale(self):
        """Advance the scale using the circle of fifths every minute."""
        if time.time() - self.scale_change_time >= 60:
            self.circle_index = (self.circle_index + 1) % len(self.circle_of_fifths)
            self.current_root_freq = self.circle_of_fifths[self.circle_index]
            self.current_scale_type = random.choice(list(self.scales.keys()))
            self.symbol_notes.clear()
            self.scale_change_time = time.time()

    @profile_function
    def assign_note(self, symbol):
        self._update_scale()
        if symbol not in self.symbol_notes:
            scale = [
                self.current_root_freq * (2 ** (interval / 12))
                for interval in self.scales[self.current_scale_type]
            ]
            note_freq = random.choice(scale)
            self.symbol_notes[symbol] = int(note_freq)

    @profile_function
    def play_note(self, symbol, muted_symbols=None):
        if muted_symbols and symbol in muted_symbols:
            return
        if symbol not in self.symbol_notes:
            self.assign_note(symbol)
        with self.lock:
            self.note_counts[symbol] += 1

    @profile_function
    def _arpeggio_loop(self):
        """Play the most active notes once per second.

        This routine collects note counts every second, then plays the top 10
        symbols by alert count. Each note is played sequentially with the audio
        queue cleared in between so that only one note is audible at a time. The
        playback rate is capped at 10 notes per second by spacing them evenly
        across the one second window.
        """
        while not self.stop_event.is_set():
            time.sleep(1.0)
            self._update_scale()
            with self.lock:
                items = list(self.note_counts.items())
                self.note_counts.clear()
            if not items:
                continue
            # Select top 10 symbols by alert count
            items.sort(key=lambda x: x[1], reverse=True)
            items = items[:10]

            notes = []
            for sym, _ in items:
                if sym not in self.symbol_notes:
                    self.assign_note(sym)
                notes.append(self.symbol_notes[sym])

            if not notes:
                continue

            # Spread the notes evenly over the next second
            interval = 1.0 / len(notes)

            for n in notes:
                # Ensure no two notes overlap
                self.audio_player.clear_queue()
                self.audio_player.play_sound(n, DURATION)
                time.sleep(interval)

    @profile_function
    def stop(self):
        self.stop_event.set()
        if self.player_thread.is_alive():
            self.player_thread.join()

################################################################
# LAZY Color Generation for 500 Charts
################################################################
@profile_function
def color_distance(color1: Tuple[str, float, float, float],
                  color2: Tuple[str, float, float, float]) -> float:
    """
    Each color tuple is (hex_color, hue, saturation, lightness).
    We'll measure color difference in HSL space, weighting hue more.
    """
    try:
        _, h1, s1, l1 = color1
        _, h2, s2, l2 = color2
    except (IndexError, ValueError) as e:
        raise ValueError("Each color tuple must have exactly four elements: (hex_color, hue, saturation, lightness)") from e

    if not (0.0 <= h1 <= 1.0 and 0.0 <= h2 <= 1.0):
        raise ValueError("Hue values must be between 0 and 1.")

    dh = min(abs(h1 - h2), 1.0 - abs(h1 - h2))
    ds = s1 - s2
    dl = l1 - l2

    distance = math.sqrt(4 * dh ** 2 + ds ** 2 + dl ** 2)
    return distance

@profile_function
def generate_single_color(existing_colors: List[Tuple[str, float, float, float]]) -> str:
    """Generate a single distinct color on-demand, avoiding existing colors."""
    max_attempts = 100
    best_color = None
    best_min_distance = -1.0
    
    for _ in range(max_attempts):
        # Random HSL values
        h = random.random()
        s = random.uniform(0.5, 0.8)
        
        # Adjust lightness based on hue
        if 180/360 <= h <= 240/360 or 230/360 <= h <= 290/360 or h <= 10/360 or h >= 350/360:
            l_val = random.uniform(0.45, 0.75)
        else:
            l_val = random.uniform(0.35, 0.65)
        
        rgb = colorsys.hls_to_rgb(h, l_val, s)
        rgb_int = tuple(int(c * 255) for c in rgb)
        hex_color = '#{:02x}{:02x}{:02x}'.format(*rgb_int)
        candidate = (hex_color, h, s, l_val)
        
        if not existing_colors:
            return hex_color
        
        # Find minimum distance to existing colors
        min_distance = min(color_distance(candidate, existing) for existing in existing_colors)
        
        if min_distance > best_min_distance:
            best_min_distance = min_distance
            best_color = hex_color
    
    return best_color if best_color else '#FFFFFF'

class LazyColorPool:
    """Lazy color generation for 500+ charts without upfront computation."""
    
    def __init__(self):
        self.colors = []  # List of (hex_color, hue, saturation, lightness) tuples
        self.used_colors = set()
        self.lock = threading.Lock()
    
    def acquire_color(self) -> str:
        """Get a color, generating it lazily if needed."""
        with self.lock:
            # Generate new color that's distinct from existing ones
            hex_color = generate_single_color(self.colors)
            
            # Store color info for future distance calculations
            r, g, b = int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)
            h, l, s = colorsys.rgb_to_hls(r/255, g/255, b/255)
            color_tuple = (hex_color, h, s, l)
            
            self.colors.append(color_tuple)
            self.used_colors.add(hex_color)
            
            return hex_color
    
    def release_color(self, color: str):
        """Release a color back to the pool."""
        with self.lock:
            if color in self.used_colors:
                self.used_colors.discard(color)
                # Keep it in self.colors for distance calculations

################################################################
# Transparent Overlay classes
################################################################
class TransparentOverlay(QWidget):
    __slots__ = ("tree_widget", "callback")
    @profile_function
    def __init__(self, tree_widget, callback):
        super().__init__(tree_widget.viewport())
        self.tree_widget = tree_widget
        self.callback = callback

        # Semi-transparent overlay
        self.setStyleSheet("background-color: rgba(120, 120, 120, 0.05);")

        self.setAttribute(Qt.WidgetAttribute.WA_NoSystemBackground, True)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint)
        self.setMouseTracking(True)
        self.resize(tree_widget.viewport().size())
        self.move(0, 0)
        self.show()

    @profile_function
    def mousePressEvent(self, event):
        item = self.tree_widget.itemAt(event.pos())
        if item:
            self.callback(item, event.button())
        event.accept()

    @profile_function
    def wheelEvent(self, event):
        QApplication.sendEvent(self.tree_widget.viewport(), event)

class OverlayedTreeWidget(QTreeWidget):
    """A QTreeWidget that holds a transparent overlay for custom clicks."""

    __slots__ = ("callback", "overlay")

    @profile_function
    def __init__(self, callback, parent=None):
        super().__init__(parent)
        self.callback = callback
        self.overlay = TransparentOverlay(self, self.callback)

        # Modern styling
        self.setAlternatingRowColors(True)
        self.setAnimated(True)
        self.setRootIsDecorated(False)
        self.setUniformRowHeights(True)

        # Row selection style
        self.setSelectionBehavior(QTreeWidget.SelectionBehavior.SelectRows)
        
        # RESPONSIVE: Set proper size policy
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    @profile_function
    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.overlay.resize(self.viewport().size())
        self.overlay.move(0, 0)

    @profile_function
    def scrollContentsBy(self, dx, dy):
        super().scrollContentsBy(dx, dy)
        self.overlay.resize(self.viewport().size())
        self.overlay.move(0, 0)

################################################################
# Modern UI Elements with Glass Effect
################################################################
class ModernButton(QPushButton):
    __slots__ = ("accent_color",)
    def __init__(self, text, parent=None, accent_color="#8b5cf6"):
        super().__init__(text, parent)
        self.accent_color = accent_color
        self.setMinimumHeight(28)
        self.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        
        # Convert hex to rgba with transparency
        color = QColor(accent_color)
        r, g, b = color.red(), color.green(), color.blue()
        
        self.setStyleSheet(f"""
            QPushButton {{
                background-color: rgba({r}, {g}, {b}, 0.2);
                color: white;
                border: 1px solid rgba(255, 255, 255, 0.2);
                border-radius: 4px;
                padding: 6px 12px;
                font-weight: 500;
                font-size: 12px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }}
            QPushButton:hover {{
                background-color: rgba({r}, {g}, {b}, 0.3);
                border: 1px solid rgba(255, 255, 255, 0.3);
                box-shadow: 0 0 20px rgba({r}, {g}, {b}, 0.4);
            }}
            QPushButton:pressed {{
                background-color: rgba({r}, {g}, {b}, 0.4);
            }}
            QPushButton:disabled {{
                background-color: rgba(176, 176, 176, 0.2);
                color: rgba(224, 224, 224, 0.5);
            }}
        """)

class ModernLineEdit(QLineEdit):
    __slots__ = ()
    def __init__(self, placeholder_text="", parent=None):
        super().__init__(parent)
        self.setPlaceholderText(placeholder_text)
        self.setMinimumHeight(28)
        self.setStyleSheet("""
            QLineEdit {
                background-color: rgba(45, 45, 45, 0.7);
                color: #E0E0E0;
                border: 1px solid rgba(255, 255, 255, 0.1);
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 12px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
            QLineEdit:focus {
                border: 1px solid #8b5cf6;
                box-shadow: 0 0 10px rgba(139, 92, 246, 0.3);
            }
            QLineEdit:hover {
                border: 1px solid rgba(139, 92, 246, 0.5);
            }
        """)

class ModernSpinBox(QSpinBox):
    __slots__ = ()
    def __init__(self, parent=None, min_value=0, max_value=100, step=1, default_value=0):
        super().__init__(parent)
        self.setRange(min_value, max_value)
        self.setSingleStep(step)
        self.setValue(default_value)
        self.setMinimumHeight(28)
        self.setStyleSheet("""
            QSpinBox {
                background-color: rgba(45, 45, 45, 0.7);
                color: #E0E0E0;
                border: 1px solid rgba(255, 255, 255, 0.1);
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 12px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
            QSpinBox:focus {
                border: 1px solid #8b5cf6;
                box-shadow: 0 0 10px rgba(139, 92, 246, 0.3);
            }
            QSpinBox:hover {
                border: 1px solid rgba(139, 92, 246, 0.5);
            }
            QSpinBox::up-button, QSpinBox::down-button {
                width: 16px;
                border-radius: 2px;
                background-color: rgba(61, 61, 61, 0.5);
            }
            QSpinBox::up-button:hover, QSpinBox::down-button:hover {
                background-color: rgba(77, 77, 77, 0.7);
            }
            QSpinBox::up-button:pressed, QSpinBox::down-button:pressed {
                background-color: rgba(93, 93, 93, 0.7);
            }
        """)

class ModernDoubleSpinBox(QDoubleSpinBox):
    __slots__ = ()
    def __init__(self, parent=None, min_value=0.0, max_value=100.0, step=0.1, default_value=0.0, decimals=2):
        super().__init__(parent)
        self.setRange(min_value, max_value)
        self.setSingleStep(step)
        self.setValue(default_value)
        self.setDecimals(decimals)
        self.setMinimumHeight(28)
        self.setStyleSheet("""
            QDoubleSpinBox {
                background-color: rgba(45, 45, 45, 0.7);
                color: #E0E0E0;
                border: 1px solid rgba(255, 255, 255, 0.1);
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 12px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
            QDoubleSpinBox:focus {
                border: 1px solid #8b5cf6;
                box-shadow: 0 0 10px rgba(139, 92, 246, 0.3);
            }
            QDoubleSpinBox:hover {
                border: 1px solid rgba(139, 92, 246, 0.5);
            }
            QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {
                width: 16px;
                border-radius: 2px;
                background-color: rgba(61, 61, 61, 0.5);
            }
            QDoubleSpinBox::up-button:hover, QDoubleSpinBox::down-button:hover {
                background-color: rgba(77, 77, 77, 0.7);
            }
            QDoubleSpinBox::up-button:pressed, QDoubleSpinBox::down-button:pressed {
                background-color: rgba(93, 93, 93, 0.7);
            }
        """)

class ModernComboBox(QComboBox):
    __slots__ = ()
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumHeight(28)
        self.setStyleSheet("""
            QComboBox {
                background-color: rgba(45, 45, 45, 0.7);
                color: #E0E0E0;
                border: 1px solid rgba(255, 255, 255, 0.1);
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 12px;
                min-width: 6em;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
            QComboBox:focus {
                border: 1px solid #8b5cf6;
                box-shadow: 0 0 10px rgba(139, 92, 246, 0.3);
            }
            QComboBox:hover {
                border: 1px solid rgba(139, 92, 246, 0.5);
            }
            QComboBox::drop-down {
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 20px;
                border-left-width: 1px;
                border-left-color: rgba(255, 255, 255, 0.1);
                border-left-style: solid;
                border-top-right-radius: 3px;
                border-bottom-right-radius: 3px;
            }
            QComboBox::down-arrow {
                image: url(:/icons/dropdown_arrow.png);
                width: 12px;
                height: 12px;
            }
            QComboBox QAbstractItemView {
                border: 1px solid rgba(255, 255, 255, 0.1);
                background-color: rgba(45, 45, 45, 0.9);
                color: #E0E0E0;
                selection-background-color: rgba(139, 92, 246, 0.4);
                selection-color: white;
            }
        """)

class ModernCheckBox(QCheckBox):
    __slots__ = ()
    def __init__(self, text, parent=None):
        super().__init__(text, parent)
        self.setStyleSheet("""
            QCheckBox {
                color: #E0E0E0;
                font-size: 12px;
                spacing: 8px;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
                border-radius: 3px;
                border: 1px solid rgba(255, 255, 255, 0.2);
                background-color: rgba(45, 45, 45, 0.5);
            }
            QCheckBox::indicator:unchecked:hover {
                border: 1px solid rgba(139, 92, 246, 0.5);
            }
            QCheckBox::indicator:checked {
                background-color: rgba(139, 92, 246, 0.7);
                border: 1px solid rgba(139, 92, 246, 0.8);
                image: url(:/icons/checkmark.png);
            }
        """)


class CardFrame(QFrame):
    """A card-like container with optional title and content layout."""

    __slots__ = (
        "title",
        "title_position",
        "title_label",
        "main_layout",
        "content_widget",
        "content_layout",
    )

    def __init__(self, parent=None, title=None, title_position="top", compact=False):
        super().__init__(parent)
        self.title = title
        self.title_position = title_position
        self.title_label = None

        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setObjectName("cardFrame")
        self.setStyleSheet("""
            #cardFrame {
                background-color: rgba(31, 34, 42, 0.8);
                border-radius: 8px;
                border: 1px solid rgba(255, 255, 255, 0.1);
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
        """)
        
        # RESPONSIVE: Set proper size policy
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        # Drop shadow with purple glow
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(20)
        drop_color = QColor("#8b5cf6")
        drop_color.setAlpha(30)
        shadow.setColor(drop_color)
        shadow.setOffset(0, 4)
        self.setGraphicsEffect(shadow)

        self.main_layout = QVBoxLayout(self)
        if compact:
            self.main_layout.setContentsMargins(8, 8, 8, 8)
            self.main_layout.setSpacing(4)
        else:
            self.main_layout.setContentsMargins(16, 16, 16, 16)
            self.main_layout.setSpacing(8)

        if self.title:
            self.title_label = QLabel(self.title)
            self.title_label.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
            self.title_label.setStyleSheet("color: #E0E0E0;")

            if self.title_position == "top":
                self.main_layout.addWidget(self.title_label)
                # Add accent line after title
                accent = AnimatedAccentLine(color="#8b5cf6", orientation="horizontal")
                self.main_layout.addWidget(accent)

        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0, 0, 0, 0)
        self.content_layout.setSpacing(4 if compact else 8)
        self.main_layout.addWidget(self.content_widget)

        if self.title and self.title_position == "bottom":
            self.main_layout.addWidget(self.title_label)

    def add_widget(self, widget):
        self.content_layout.addWidget(widget)

    def add_layout(self, layout):
        self.content_layout.addLayout(layout)

    def add_spacer(self):
        self.content_layout.addSpacerItem(QSpacerItem(0, 0, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding))

################################################################
# Main Window
################################################################
class HFTInteractiveNewsClient(QMainWindow):
    class TimeAxisItem(pg.AxisItem):
        """A custom time axis for pyqtgraph, showing HH:MM:SS in UTC."""

        @profile_function
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        @profile_function
        def tickStrings(self, values, scale, spacing):
            ny_tz = pytz.timezone("America/New_York")
            return [datetime.fromtimestamp(value, pytz.UTC).astimezone(ny_tz).strftime('%H:%M:%S') for value in values]

    @profile_function
    def __init__(self):
        super().__init__()
        self.symbol_mapping = defaultdict(set)
        # Initialize selected_ric early so dependent UI elements can
        # safely reference it during setup
        self.selected_ric = None
        self.setWindowTitle("HFT Interactive News Feed - 500 Chart Config - RESPONSIVE")
        self.setGeometry(100, 100, 1600, 900)
        
        # RESPONSIVE: Set minimum window size to prevent over-shrinking
        self.setMinimumSize(1200, 700)

        # Make window frameless
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, False)

        # Load custom fonts
        self.load_fonts()

        self.event_bus = EventBus()
        self.threadpool = QThreadPool()
        self.process_pool = ProcessPoolExecutor(max_workers=multiprocessing.cpu_count())
        self.trading_mode_enabled = False
        self.y_axis_mode = 'gain'
        self.chart_history_seconds = DEFAULT_CHART_HISTORY_SECONDS
        self.clock_mode = CLOCK_MODE_WORLD

        # Dark theme with transparency
        self.apply_modern_dark_theme()

        # Window dragging support
        self.drag_pos = None

        # Delayed init
        QTimer.singleShot(100, self.delayed_init)

    def resizeEvent(self, event):
        super().resizeEvent(event)

    def mousePressEvent(self, event):
        """Enable window dragging"""
        if event.button() == Qt.MouseButton.LeftButton:
            # Check if we're clicking on the header area
            if event.position().y() < 80:  # Header height
                self.drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
                event.accept()

    def mouseMoveEvent(self, event):
        """Handle window dragging"""
        if event.buttons() == Qt.MouseButton.LeftButton and self.drag_pos:
            self.move(event.globalPosition().toPoint() - self.drag_pos)
            event.accept()

    def mouseReleaseEvent(self, event):
        """Reset drag position"""
        self.drag_pos = None

    ##########################################################
    # Popups replaced with no-op notifications
    ##########################################################
    @profile_function
    def show_notification(self, message, type_="info", duration=3000):
        """No-op placeholder to keep calls consistent, but no popups shown."""
        logging.info(f"[NO-POPUP] {type_.upper()} - {message}")

    @profile_function
    def delayed_init(self):
        self.setup_ui()
        self.setup_data_structures()
        self.setup_event_handlers()
        self.setup_async_tasks()
        self.charts_warned = False

        self.last_top_tree_update = 0
        self.text_item_positions = {}

        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.flush_ui_updates)
        self.update_timer.start(200)

        self.clock_timer = QTimer()
        self.clock_timer.timeout.connect(self.update_clock_label)
        self.clock_timer.timeout.connect(self.update_world_clock_label)
        self.clock_timer.start(1)

        # Keyboard shortcuts
        self.setup_shortcuts()

        # Track last hovered symbol for chart hover highlighting
        self.last_hovered_symbol = None

    @profile_function
    def setup_shortcuts(self):
        refresh_shortcut = QShortcut(QKeySequence(Qt.Key.Key_F5), self)
        refresh_shortcut.activated.connect(self.refresh_data)

        trading_shortcut = QShortcut(QKeySequence("Ctrl+T"), self)
        trading_shortcut.activated.connect(self.toggle_trading_mode)

        f12_shortcut = QShortcut(QKeySequence(Qt.Key.Key_F12), self)
        f12_shortcut.activated.connect(self.toggle_trading_mode)

        chart_shortcut = QShortcut(QKeySequence("Ctrl+1"), self)
        chart_shortcut.activated.connect(lambda: self.news_frame.setCurrentIndex(0))

        table_shortcut = QShortcut(QKeySequence("Ctrl+2"), self)
        table_shortcut.activated.connect(lambda: self.news_frame.setCurrentIndex(1))

        mute_toggle = QShortcut(QKeySequence("M"), self)
        mute_toggle.activated.connect(self.toggle_mute_all_hotkey)

        axis_toggle = QShortcut(QKeySequence("C"), self)
        axis_toggle.activated.connect(self.toggle_y_axis_mode_hotkey)

        ticker_focus = QShortcut(QKeySequence("`"), self)
        ticker_focus.activated.connect(self.focus_ticker_entry)

        left_tab = QShortcut(QKeySequence(Qt.Key.Key_Left), self)
        left_tab.activated.connect(lambda: self.switch_tab(-1))

        right_tab = QShortcut(QKeySequence(Qt.Key.Key_Right), self)
        right_tab.activated.connect(lambda: self.switch_tab(1))

        up_tab = QShortcut(QKeySequence(Qt.Key.Key_Up), self)
        up_tab.activated.connect(self.scroll_active_tree_top)

        down_tab = QShortcut(QKeySequence(Qt.Key.Key_Down), self)
        down_tab.activated.connect(self.scroll_active_tree_bottom)

        home_key = QShortcut(QKeySequence(Qt.Key.Key_Home), self)
        home_key.activated.connect(self.scroll_active_tree_top)

        end_key = QShortcut(QKeySequence(Qt.Key.Key_End), self)
        end_key.activated.connect(self.scroll_active_tree_bottom)

        page_up_key = QShortcut(QKeySequence(Qt.Key.Key_PageUp), self)
        page_up_key.activated.connect(self.scroll_active_tree_page_up)

        page_down_key = QShortcut(QKeySequence(Qt.Key.Key_PageDown), self)
        page_down_key.activated.connect(self.scroll_active_tree_page_down)

        select_top_key = QShortcut(QKeySequence("T"), self)
        select_top_key.activated.connect(self.select_active_tree_top_item)

        select_bottom_key = QShortcut(QKeySequence("B"), self)
        select_bottom_key.activated.connect(self.select_active_tree_bottom_item)

        delete_key = QShortcut(QKeySequence(Qt.Key.Key_Delete), self)
        delete_key.activated.connect(self.delete_selected_item)

    @profile_function
    def refresh_data(self):
        self.show_notification("Refreshing data...", "info")
        QTimer.singleShot(0, self.update_all_data)

    @profile_function
    def update_all_data(self):
        self.event_bus.publish(Event('CHART_UPDATE', None))
        QTimer.singleShot(0, lambda: self.update_top_tree_display(force=True))
        self.show_notification("Data refreshed successfully", "success")

    @profile_function
    def load_fonts(self):
        QFontDatabase.addApplicationFont(":/fonts/Roboto-Regular.ttf")
        QFontDatabase.addApplicationFont(":/fonts/Roboto-Bold.ttf")
        QFontDatabase.addApplicationFont(":/fonts/Roboto-Medium.ttf")

        self.app_font = QFont("Roboto", 11)
        self.app_font_bold = QFont("Roboto", 11, QFont.Weight.Bold)
        self.app_font_medium = QFont("Roboto", 11)
        self.app_font_medium.setWeight(QFont.Weight.Medium)

        self.heading_font = QFont("Roboto", 16, QFont.Weight.Bold)
        self.subheading_font = QFont("Roboto", 12, QFont.Weight.Medium)

    @profile_function
    def apply_modern_dark_theme(self):
        """Applies a consistent dark theme."""
        self.primary_color = "#8b5cf6"
        self.primary_variant = "#7c3aed"
        self.secondary_color = "#3b82f6"
        self.teal_accent = "#06b6d4"
        self.background_color = "rgba(18, 18, 18, 0.85)"
        self.surface_color = "rgba(30, 30, 30, 0.85)"
        self.error_color = "#ef4444"
        self.success_color = "#10b981"
        self.warning_color = "#f59e0b"
        self.info_color = "#3b82f6"
        self.on_primary = "#FFFFFF"
        self.on_secondary = "#000000"
        self.on_background = "#E0E0E0"
        self.on_surface = "#E0E0E0"
        self.on_error = "#FFFFFF"
        self.disabled_color = "#757575"
        
        # Base stylesheet
        self.setStyleSheet(f"""
            QMainWindow {{
                background-color: #0f1117;
                color: {self.on_background};
                font-family: 'Roboto';
                font-size: 10pt;
            }}
            
            QWidget {{
                background-color: transparent;
                color: {self.on_background};
            }}
            
            #centralWidget {{
                background-color: #121212;
                border-radius: 12px;
                border: 1px solid rgba(255, 255, 255, 0.1);
            }}

            QSplitter::handle {{
                background-color: rgba(45, 45, 45, 0.5);
                height: 1px;
                margin: 1px 0px 1px 0px;
            }}
            QSplitter::handle:hover {{
                background-color: {self.primary_color};
            }}

            QTabWidget::pane {{
                border: 1px solid rgba(58, 58, 58, 0.5);
                border-radius: 4px;
                background-color: rgba(30, 30, 30, 0.7);
                top: -1px;
            }}
            QTabBar::tab {{
                background-color: rgba(45, 45, 45, 0.5);
                color: {self.on_background};
                padding: 8px 16px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                margin-right: 2px;
                font-size: 10pt;
            }}
            QTabBar::tab:selected {{
                background-color: rgba(139, 92, 246, 0.3);
                color: {self.on_primary};
                font-weight: bold;
                border: 1px solid rgba(255, 255, 255, 0.2);
            }}
            QTabBar::tab:hover:!selected {{
                background-color: rgba(61, 61, 61, 0.5);
            }}

            QTreeWidget {{
                background-color: rgba(38, 38, 38, 0.7);
                alternate-background-color: rgba(48, 48, 48, 0.7);
                color: {self.on_surface};
                border: none;
                border-radius: 4px;
                font-size: 10pt;
            }}
            QTreeWidget::item {{
                min-height: 28px;
                padding: 2px;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            }}
            QTreeWidget::item:selected {{
                background-color: rgba(139, 92, 246, 0.4);
                color: {self.on_primary};
            }}
            QTreeWidget::item:hover:!selected {{
                background-color: rgba(255, 255, 255, 0.05);
            }}
            QHeaderView::section {{
                background-color: rgba(45, 45, 45, 0.8);
                color: {self.on_background};
                padding: 6px;
                border: none;
                border-right: 1px solid rgba(255, 255, 255, 0.1);
                font-weight: bold;
            }}

            QListWidget {{
                background-color: rgba(38, 38, 38, 0.7);
                color: {self.on_surface};
                border: none;
                border-radius: 4px;
                font-size: 10pt;
                padding: 4px;
            }}
            QListWidget::item {{
                padding: 6px;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
                min-height: 20px;
            }}
            QListWidget::item:selected {{
                background-color: rgba(139, 92, 246, 0.4);
                color: {self.on_primary};
            }}
            QListWidget::item:hover:!selected {{
                background-color: rgba(255, 255, 255, 0.05);
            }}

            QLabel {{
                color: {self.on_background};
                font-size: 10pt;
                background-color: transparent;
            }}

            QScrollBar:vertical {{
                background: rgba(45, 45, 45, 0.5);
                width: 10px;
                margin: 0px;
                border-radius: 5px;
            }}
            QScrollBar::handle:vertical {{
                background: rgba(139, 92, 246, 0.5);
                min-height: 30px;
                border-radius: 5px;
            }}
            QScrollBar::handle:vertical:hover {{
                background: rgba(139, 92, 246, 0.7);
            }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
                height: 0px;
            }}

            QScrollBar:horizontal {{
                background: rgba(45, 45, 45, 0.5);
                height: 10px;
                margin: 0px;
                border-radius: 5px;
            }}
            QScrollBar::handle:horizontal {{
                background: rgba(139, 92, 246, 0.5);
                min-width: 30px;
                border-radius: 5px;
            }}
            QScrollBar::handle:horizontal:hover {{
                background: rgba(139, 92, 246, 0.7);
            }}
            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
                width: 0px;
            }}

            QFrame[frameShape="4"], QFrame[frameShape="5"], QFrame[frameShape="6"] {{
                background-color: rgba(58, 58, 58, 0.5);
            }}

            QMenu {{
                background-color: rgba(45, 45, 45, 0.9);
                color: {self.on_background};
                border: 1px solid rgba(255, 255, 255, 0.2);
                border-radius: 4px;
                padding: 5px;
            }}
            QMenu::item {{
                padding: 5px 20px 5px 20px;
                border-radius: 2px;
            }}
            QMenu::item:selected {{
                background-color: rgba(139, 92, 246, 0.4);
                color: {self.on_primary};
            }}
            QMenu::separator {{
                height: 1px;
                background-color: rgba(255, 255, 255, 0.1);
                margin: 5px 0px;
            }}

            QToolTip {{
                background-color: rgba(45, 45, 45, 0.9);
                color: {self.on_background};
                border: 1px solid rgba(139, 92, 246, 0.5);
                border-radius: 4px;
                padding: 5px;
            }}
        """)

    @profile_function
    def setup_ui(self):
        self.central_widget = QWidget()
        self.central_widget.setObjectName("centralWidget")
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget)
        self.main_layout.setSpacing(8)
        self.main_layout.setContentsMargins(12, 12, 12, 12)

        # Header
        self.setup_header()

        # Main splitter
        self.main_splitter = QSplitter(Qt.Orientation.Vertical)
        self.main_splitter.setHandleWidth(2)
        self.main_splitter.setChildrenCollapsible(False)

        # Top area (news content)
        self.setup_news_display_area()

        # Main content area (charts + tabs)
        self.setup_main_content_area()

        self.main_splitter.addWidget(self.top_control_frame)
        self.main_splitter.addWidget(self.content_area)
        
        # RESPONSIVE: Set stretch factors (1:3 ratio by default)
        self.main_splitter.setStretchFactor(0, 1)
        self.main_splitter.setStretchFactor(1, 3)
        
        # Set initial sizes (flexible, not absolute)
        self.main_splitter.setSizes([250, 750])
        
        self.main_layout.addWidget(self.main_splitter)

        # Control panel bottom
        self.setup_control_panel()

    @profile_function
    def setup_header(self):
        self.header_frame = QFrame()
        self.header_frame.setObjectName("headerFrame")
        self.header_frame.setStyleSheet("""
            #headerFrame {
                background-color: rgba(33, 37, 43, 0.8);
                border-radius: 6px;
                margin-bottom: 8px;
                border: 1px solid rgba(255, 255, 255, 0.1);
                backdrop-filter: blur(20px);
                -webkit-backdrop-filter: blur(20px);
            }
        """)
        
        # RESPONSIVE: Set size policy
        self.header_frame.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.header_frame.setMinimumHeight(60)
        self.header_frame.setMaximumHeight(80)
        
        # Add glow effect to the header
        glow = QGraphicsDropShadowEffect()
        glow.setBlurRadius(30)
        glow.setColor(QColor(139, 92, 246, 60))  # Purple glow
        glow.setOffset(0, 0)
        self.header_frame.setGraphicsEffect(glow)
        
        self.header_layout = QHBoxLayout(self.header_frame)
        self.header_layout.setContentsMargins(12, 8, 12, 8)

        self.logo_label = QLabel()
        pixel_map = QPixmap(16, 16)
        pixel_map.fill(QColor(self.primary_color))
        self.logo_label.setPixmap(pixel_map)
        self.header_layout.addWidget(self.logo_label)

        self.title_label = QLabel("Filterfeedv2.9.9 [500 CHARTS - RESPONSIVE]")
        self.title_label.setFont(self.heading_font)
        title_shadow = QGraphicsDropShadowEffect(self.title_label)
        title_shadow.setBlurRadius(8)
        title_shadow.setColor(QColor(self.primary_color))
        title_shadow.setOffset(0, 1)
        self.title_label.setGraphicsEffect(title_shadow)
        self.header_layout.addWidget(self.title_label)

        self.header_layout.addStretch()

        self.connection_layout = QHBoxLayout()
        self.connection_layout.setSpacing(8)

        self.connection_status = QLabel()
        self.connection_status.setFixedSize(12, 12)
        self.connection_status.setStyleSheet("background-color: red; border-radius: 6px;")
        self.connection_layout.addWidget(self.connection_status)

        self.status_label = QLabel("Disconnected")
        self.status_label.setFont(self.app_font)
        self.connection_layout.addWidget(self.status_label)

        self.debug_label = QLabel("Debug: No updates | Messages/s: 0.00")
        self.debug_label.setStyleSheet("color: #BBBBBB; font-size: 10px;")
        self.connection_layout.addWidget(self.debug_label)

        self.clock_label = QLabel()
        self.clock_label.setStyleSheet("color: #BBBBBB; font-size: 10px;")
        self.connection_layout.addWidget(self.clock_label)

        self.world_clock_label = QLabel()
        self.world_clock_label.setStyleSheet("color: #BBBBBB; font-size: 12px;")
        self.connection_layout.addWidget(self.world_clock_label)

        self.clock_label.mousePressEvent = lambda e: self.cycle_clock_mode()
        self.world_clock_label.mousePressEvent = lambda e: self.cycle_clock_mode()

        # Default to NYC 12h clock with milliseconds
        self.on_clock_mode_changed(2)

        self.header_layout.addLayout(self.connection_layout)

        # Window controls
        self.setup_window_controls()

        self.main_layout.addWidget(self.header_frame)

    def setup_window_controls(self):
        """Add minimize, maximize, close buttons"""
        self.window_controls = QWidget(self.header_frame)
        controls_layout = QHBoxLayout(self.window_controls)
        controls_layout.setSpacing(5)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        
        # Minimize button
        self.min_button = QPushButton("−")
        self.min_button.setFixedSize(30, 30)
        self.min_button.clicked.connect(self.showMinimized)
        
        # Maximize button
        self.max_button = QPushButton("□")
        self.max_button.setFixedSize(30, 30)
        self.max_button.clicked.connect(self.toggle_maximize)
        
        # Close button
        self.close_button = QPushButton("×")
        self.close_button.setFixedSize(30, 30)
        self.close_button.clicked.connect(self.close)
        
        for btn in [self.min_button, self.max_button, self.close_button]:
            btn.setStyleSheet("""
                QPushButton {
                    background-color: rgba(255, 255, 255, 0.1);
                    border: none;
                    border-radius: 15px;
                    color: white;
                    font-size: 16px;
                }
                QPushButton:hover {
                    background-color: rgba(255, 255, 255, 0.2);
                }
            """)
        
        self.close_button.setStyleSheet("""
            QPushButton {
                background-color: rgba(239, 68, 68, 0.3);
                border: none;
                border-radius: 15px;
                color: white;
                font-size: 16px;
            }
            QPushButton:hover {
                background-color: rgba(239, 68, 68, 0.5);
            }
        """)
        
        controls_layout.addWidget(self.min_button)
        controls_layout.addWidget(self.max_button)
        controls_layout.addWidget(self.close_button)
        
        # Position in header
        self.header_layout.addWidget(self.window_controls)

    def toggle_maximize(self):
        if self.isMaximized():
            self.showNormal()
            self.max_button.setText("□")
        else:
            self.showMaximized()
            self.max_button.setText("❐")

    @profile_function
    def setup_news_display_area(self):
        self.top_control_frame = QWidget()
        self.top_control_frame.setObjectName("topControlFrame")
        self.top_control_frame.setStyleSheet("""
            #topControlFrame {
                background-color: rgba(33, 37, 43, 0.85);
                border-radius: 6px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.05);
            }
        """)
        
        # RESPONSIVE: Set size policy
        self.top_control_frame.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        self.top_control_layout = QVBoxLayout(self.top_control_frame)
        self.top_control_layout.setSpacing(4)
        self.top_control_layout.setContentsMargins(8, 8, 8, 8)

        self.news_content_widget = QWidget()
        
        # RESPONSIVE: Set size policy
        self.news_content_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        self.news_content_layout = QVBoxLayout(self.news_content_widget)
        self.news_content_layout.setContentsMargins(0, 0, 0, 0)
        self.news_content_layout.setSpacing(6)

        # Headline section
        self.headline_card = QFrame()
        self.headline_card.setObjectName("headlineCard")
        self.headline_card.setStyleSheet("""
            #headlineCard {
                background-color: rgba(42, 42, 42, 0.7);
                border-radius: 4px;
                border-left: 3px solid transparent;
                padding: 4px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
        """)
        
        # RESPONSIVE: Set size policy
        self.headline_card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        
        headline_layout = QVBoxLayout(self.headline_card)
        headline_layout.setContentsMargins(8, 8, 8, 8)
        headline_layout.setSpacing(2)

        headline_label = QLabel("Headline")
        headline_label.setStyleSheet("""
            font-weight: bold;
            color: #a78bfa;
            font-size: 13px;
        """)
        headline_layout.addWidget(headline_label)

        self.headline_label = QLabel("Select a news item to view details here.")
        self.headline_label.setFont(QFont("Roboto", 12, QFont.Weight.Bold))
        self.headline_label.setWordWrap(True)
        self.headline_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.headline_label.setStyleSheet("color: white; background-color: transparent;")
        
        # RESPONSIVE: Set size policy
        self.headline_label.setMinimumHeight(40)
        self.headline_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        headline_scroll = QScrollArea()
        headline_scroll.setWidgetResizable(True)
        headline_scroll.setStyleSheet("border: none; background-color: transparent;")
        
        # RESPONSIVE: Set size policy
        headline_scroll.setMinimumHeight(60)
        headline_scroll.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        headline_container = QWidget()
        headline_container_layout = QVBoxLayout(headline_container)
        headline_container_layout.setContentsMargins(0, 0, 0, 0)
        headline_container_layout.addWidget(self.headline_label)
        headline_container_layout.addStretch()

        headline_scroll.setWidget(headline_container)
        headline_layout.addWidget(headline_scroll)

        self.news_content_layout.addWidget(self.headline_card)

        # Main content area
        main_content = QHBoxLayout()
        main_content.setSpacing(6)

        # Left column
        left_column = QVBoxLayout()
        left_column.setSpacing(6)

        meta_card = QFrame()
        meta_card.setObjectName("metaCard")
        meta_card.setStyleSheet("""
            #metaCard {
                background-color: rgba(42, 42, 42, 0.7);
                border-radius: 4px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
        """)
        
        # RESPONSIVE: Set size policy
        meta_card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        
        meta_layout = QGridLayout(meta_card)
        meta_layout.setContentsMargins(8, 8, 8, 8)
        meta_layout.setHorizontalSpacing(12)
        meta_layout.setVerticalSpacing(4)

        meta_fields = [
            {"label": "Timestamp:", "var_name": "timestamp_value", "default": "N/A"},
            {"label": "Source:", "var_name": "source_value", "default": "N/A"},
            {"label": "Primary RIC:", "var_name": "ric1_value", "default": "none"},
            {"label": "Secondary RIC:", "var_name": "ric2_value", "default": "none"}
        ]

        for i, field in enumerate(meta_fields):
            row, col = i // 2, i % 2
            label = QLabel(field["label"])
            label.setStyleSheet("""
                color: #BBBBBB;
                font-weight: bold;
                font-size: 11px;
            """)
            value = QLabel(field["default"])
            value.setStyleSheet("""
                color: white;
                font-size: 11px;
            """)
            setattr(self, field["label"].lower().replace(":", "").strip() + "_label", label)
            setattr(self, field["var_name"], value)
            meta_layout.addWidget(label, row, col*2)
            meta_layout.addWidget(value, row, col*2+1)

        left_column.addWidget(meta_card)

        # Additional company values
        self.additional_company_values = {}

        company_panel = QFrame()
        company_panel.setObjectName("companyPanel")
        company_panel.setStyleSheet("""
            #companyPanel {
                background-color: rgba(42, 42, 42, 0.7);
                border-radius: 4px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
        """)
        
        # RESPONSIVE: Set size policy
        company_panel.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        company_layout = QVBoxLayout(company_panel)
        company_layout.setContentsMargins(8, 8, 8, 8)
        company_layout.setSpacing(4)

        company_header = QHBoxLayout()
        company_header.setSpacing(8)

        company_label = QLabel("Company:")
        company_label.setStyleSheet("color: #BBBBBB; font-weight: bold; font-size: 11px;")
        self.company_name_value = QLabel("N/A")
        self.company_name_value.setStyleSheet("color: white; font-size: 12px; font-weight: bold;")
        company_header.addWidget(company_label)
        company_header.addWidget(self.company_name_value, 1)

        type_label = QLabel("Type:")
        type_label.setStyleSheet("color: #BBBBBB; font-weight: bold; font-size: 10px;")
        self.additional_company_values["Type"] = QLabel("N/A")
        self.additional_company_values["Type"].setStyleSheet("color: white; font-size: 11px;")
        company_header.addWidget(type_label)
        company_header.addWidget(self.additional_company_values["Type"])

        company_layout.addLayout(company_header)

        details_grid = QGridLayout()
        details_grid.setHorizontalSpacing(12)
        details_grid.setVerticalSpacing(4)

        detail_fields = [
            {"label": "Market Cap:", "key": "Market Cap"},
            {"label": "Currency:", "key": "Currency Name"},
            {"label": "Primary Exchange:", "key": "Primary Exchange"},
            {"label": "Employees:", "key": "Total Employees"},
            {"label": "List Date:", "key": "List Date"},
            {"label": "Shares Outstanding:", "key": "Share Class Shares Outstanding"},
            {"label": "Weighted Shares:", "key": "Weighted Shares Outstanding"}
        ]

        for i, field in enumerate(detail_fields):
            row, col = i // 3, i % 3
            label = QLabel(field["label"])
            label.setStyleSheet("color: #BBBBBB; font-weight: bold; font-size: 11px;")
            value = QLabel("N/A")
            value.setStyleSheet("color: white; font-size: 11px;")
            self.additional_company_values[field["key"]] = value
            details_grid.addWidget(label, row, col*2)
            details_grid.addWidget(value, row, col*2+1)

        company_layout.addLayout(details_grid)

        desc_section = QFrame()
        desc_section.setObjectName("descSection")
        desc_section.setStyleSheet("""
            #descSection {
                background-color: rgba(36, 36, 36, 0.5);
                border-radius: 3px;
                backdrop-filter: blur(5px);
                -webkit-backdrop-filter: blur(5px);
            }
        """)
        
        # RESPONSIVE: Set size policy
        desc_section.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        desc_layout = QVBoxLayout(desc_section)
        desc_layout.setContentsMargins(6, 6, 6, 6)
        desc_layout.setSpacing(4)

        desc_label = QLabel("Description:")
        desc_label.setStyleSheet("color: #BBBBBB; font-weight: bold; font-size: 11px;")
        desc_layout.addWidget(desc_label)

        self.description_value = QLabel("N/A")
        self.description_value.setStyleSheet("color: white; font-size: 11px;")
        self.description_value.setWordWrap(True)
        self.description_value.setAlignment(
            Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft
        )
        
        # RESPONSIVE: Set size policy
        self.description_value.setMinimumHeight(80)
        self.description_value.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        desc_scroll = QScrollArea()
        desc_scroll.setWidgetResizable(True)
        desc_scroll.setStyleSheet("border: none; background-color: transparent;")
        
        # RESPONSIVE: Set size policy
        desc_scroll.setMinimumHeight(80)
        desc_scroll.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        desc_container = QWidget()
        desc_container_layout = QVBoxLayout(desc_container)
        desc_container_layout.setContentsMargins(0, 0, 0, 0)
        desc_container_layout.addWidget(self.description_value)
        desc_container_layout.addStretch()

        desc_scroll.setWidget(desc_container)
        desc_layout.addWidget(desc_scroll)

        company_layout.addWidget(desc_section)
        left_column.addWidget(company_panel)
        left_column.addStretch()

        # Right column - recent headlines
        right_column = QVBoxLayout()
        right_column.setSpacing(6)

        recent_headlines_section = QFrame()
        recent_headlines_section.setObjectName("recentHeadlinesSection")
        recent_headlines_section.setStyleSheet("""
            #recentHeadlinesSection {
                background-color: rgba(42, 42, 42, 0.7);
                border-radius: 4px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
        """)
        
        # RESPONSIVE: Set size policy
        recent_headlines_section.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        recent_layout = QVBoxLayout(recent_headlines_section)
        recent_layout.setContentsMargins(8, 8, 8, 8)
        recent_layout.setSpacing(4)

        recent_header = QLabel("Recent Headlines")
        recent_header.setStyleSheet("""
            font-weight: bold;
            color: #a78bfa;
            font-size: 11px;
        """)
        recent_layout.addWidget(recent_header)

        self.recent_headlines_widget = QListWidget()
        self.recent_headlines_widget.setObjectName("recentHeadlines")
        self.recent_headlines_widget.setStyleSheet("""
            #recentHeadlines {
                background-color: rgba(38, 38, 38, 0.5);
                border-radius: 3px;
                padding: 2px;
                font-size: 11px;
            }
            QListWidget::item {
                padding: 4px;
                min-height: 20px;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            }
            QListWidget::item:hover {
                background-color: rgba(255, 255, 255, 0.1);
            }
        """)
        self.recent_headlines_widget.setSelectionMode(QListWidget.SelectionMode.NoSelection)
        
        # RESPONSIVE: Set size policy
        self.recent_headlines_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        recent_layout.addWidget(self.recent_headlines_widget)

        right_column.addWidget(recent_headlines_section)

        main_content.addLayout(left_column, 6)
        main_content.addLayout(right_column, 4)
        self.news_content_layout.addLayout(main_content)

        self.top_control_layout.addWidget(self.news_content_widget)

    @profile_function
    def update_recent_headlines_widget(self, ric):
        self.recent_headlines_widget.clear()
        if not ric or ric not in self.recent_headlines_per_ric:
            self.recent_headlines_widget.addItem("No recent headlines available for this symbol")
            return
        headlines = self.recent_headlines_per_ric[ric]
        for headline in headlines:
            item = QListWidgetItem(headline)
            item.setToolTip(headline)
            self.recent_headlines_widget.addItem(item)
        if self.recent_headlines_widget.count() > 0:
            logging.info(f"[ADDED LOG] Updated headlines widget for {ric}")

    @profile_function
    def reset_headline_highlight(self):
        self.headline_card.setStyleSheet("""
            #headlineCard {
                background-color: rgba(42, 42, 42, 0.7);
                border-radius: 4px;
                border-left: 3px solid transparent;
                padding: 4px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
        """)

    @profile_function
    def update_selected_news_display(self, headline, timestamp, source, ric1, ric2, ric3):
        logging.debug(f"[DISPLAY UPDATE] Setting headline '{headline}' with RICs {ric1},{ric2},{ric3} at {datetime.utcnow()}")
        self.headline_label.setText(headline)
        self.timestamp_value.setText("N/A" if timestamp == "N/A" else timestamp)
        self.source_value.setText("N/A" if source == "N/A" else source)
        self.ric1_value.setText(ric1)
        self.ric2_value.setText(ric2)

        # Highlight by changing only the border colour to avoid layout shifts
        self.headline_card.setStyleSheet("""
            #headlineCard {
                background-color: rgba(42, 42, 42, 0.7);
                border-radius: 4px;
                border-left: 3px solid #a78bfa;
                padding: 4px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
        """)
        QTimer.singleShot(2000, self.reset_headline_highlight)

    @profile_function
    async def handle_selected_news(self, ticker: str):
        # This version is the final one overshadowing the earlier partial
        self.company_name_value.setText("Loading...")
        for value_label in self.additional_company_values.values():
            value_label.setText("...")
        self.description_value.setText("Fetching company information...")

        formatted_date = datetime.utcnow().strftime('%Y-%m-%d')
        company_details = await self.get_polygon_data(ticker, formatted_date)
        logging.debug(f"Company details for {ticker}: {company_details}")

        if company_details:
            self.company_name_value.setText(company_details.get('name', 'N/A'))
            self.additional_company_values["Type"].setText(company_details.get('type', 'N/A'))

            raw_market_cap = company_details.get('market_cap', 'N/A')
            raw_share_class = company_details.get('share_class_shares_outstanding', 'N/A')
            raw_weighted_shares = company_details.get('weighted_shares_outstanding', 'N/A')

            formatted_market_cap = self.format_large_number(raw_market_cap if raw_market_cap != 'N/A' else raw_market_cap)
            formatted_share_class = self.format_large_number(raw_share_class if raw_share_class != 'N/A' else raw_share_class)
            formatted_weighted_shares = self.format_large_number(raw_weighted_shares if raw_weighted_shares != 'N/A' else raw_weighted_shares)

            self.additional_company_values["Market Cap"].setText(formatted_market_cap)
            self.additional_company_values["Share Class Shares Outstanding"].setText(formatted_share_class)
            self.additional_company_values["Weighted Shares Outstanding"].setText(formatted_weighted_shares)
            self.additional_company_values["Currency Name"].setText(company_details.get('currency_name', 'N/A'))
            self.additional_company_values["Primary Exchange"].setText(company_details.get('primary_exchange', 'N/A'))
            self.additional_company_values["Total Employees"].setText(str(company_details.get('total_employees', 'N/A')))
            self.additional_company_values["List Date"].setText(company_details.get('list_date', 'N/A'))

            description = company_details.get('description', 'No description available')
            if description:
                self.description_value.setText(description)
                if len(description) > 300:
                    font = self.description_value.font()
                    font.setPointSize(11)
                    self.description_value.setFont(font)
                else:
                    font = self.description_value.font()
                    font.setPointSize(11)
                    self.description_value.setFont(font)
            else:
                self.description_value.setText("No description available")

            self.show_notification(f"Loaded company details for {ticker}", "success", 2000)
        else:
            self.company_name_value.setText("N/A")
            for value_label in self.additional_company_values.values():
                value_label.setText("N/A")
            self.description_value.setText("No company information available for this ticker")
            self.debug_label.setText(f"No company details available for {ticker}")
            self.show_notification(f"No company details found for {ticker}", "warning", 2000)

        QTimer.singleShot(0, lambda symbol=ticker: self.update_recent_headlines_widget(symbol))

    @profile_function
    def setup_main_content_area(self):
        self.content_area = QWidget()
        self.content_area.setObjectName("contentArea")
        self.content_area.setStyleSheet("""
            #contentArea {
                background-color: rgba(31, 34, 42, 0.85);
                border-radius: 6px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.05);
            }
        """)
        
        # RESPONSIVE: Set size policy
        self.content_area.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        self.content_layout = QVBoxLayout(self.content_area)
        self.content_layout.setSpacing(0)
        self.content_layout.setContentsMargins(16, 16, 16, 16)

        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self.splitter.setHandleWidth(2)
        self.splitter.setChildrenCollapsible(False)

        self.setup_chart_area()
        self.setup_news_tabs()

        self.splitter.addWidget(self.chart_frame)
        self.splitter.addWidget(self.news_frame)

        # RESPONSIVE: Set stretch factors (2:5 ratio)
        self.splitter.setStretchFactor(0, 2)
        self.splitter.setStretchFactor(1, 5)
        
        # Set initial sizes (flexible, not absolute)
        self.splitter.setSizes([400, 1000])
        
        self.content_layout.addWidget(self.splitter)

    @profile_function
    def setup_chart_area(self):
        self.chart_frame = CardFrame(title="Real-time Price Changes")
        
        # RESPONSIVE: Set size policy
        self.chart_frame.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Make the chart background semi-transparent
        self.chart_frame.setStyleSheet("""
            #cardFrame {
                background-color: rgba(26, 26, 26, 0.7);
                border-radius: 8px;
                border: 1px solid rgba(255, 255, 255, 0.1);
                backdrop-filter: blur(20px);
                -webkit-backdrop-filter: blur(20px);
            }
        """)
        
        self.chart_layout = self.chart_frame.content_layout

        self.plot_widget = pg.PlotWidget(axisItems={'bottom': self.TimeAxisItem(orientation='bottom')})
        pg.setConfigOptions(antialias=True)
        
        # RESPONSIVE: Set size policy
        self.plot_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Set a semi-transparent background for the plot
        self.plot_widget.setBackground(QColor(26, 26, 26, 180))  # RGBA
        self.plot_widget.showGrid(x=True, y=True, alpha=0.1)
        
        y_label = 'Cumulative Percent Gain' if self.y_axis_mode == 'gain' else 'Cumulative Volume × Price'
        self.plot_widget.setLabel('right', y_label, color='#CCCCCC')
        self.plot_widget.setLabel('bottom', 'NEW YORK TIME', units='s', color='#CCCCCC')

        axis_pen = pg.mkPen(color='#CCCCCC', width=1)
        self.plot_widget.getAxis('right').setPen(axis_pen)
        self.plot_widget.getAxis('bottom').setPen(axis_pen)
        self.plot_widget.getAxis('right').setTextPen('#CCCCCC')
        self.plot_widget.getAxis('bottom').setTextPen('#CCCCCC')
        self.plot_widget.getAxis('right').setGrid(True)
        self.plot_widget.getAxis('bottom').setGrid(True)

        self.chart_layout.addWidget(self.plot_widget)
        self.plot_widget.scene().sigMouseClicked.connect(self.on_plot_click)
        self.plot_widget.scene().sigMouseMoved.connect(self.on_plot_hover)

        self.plot_curves = {}
        self.text_items = {}

    @profile_function
    def setup_news_tabs(self):
        self.news_frame = QTabWidget()
        self.news_frame.setObjectName("newsTabWidget")
        self.news_frame.setStyleSheet("""
            #newsTabWidget {
                background-color: rgba(38, 38, 38, 0.8);
                border-radius: 6px;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }
            QTabWidget::pane {
                background-color: rgba(30, 30, 30, 0.7);
                border: 1px solid rgba(255, 255, 255, 0.1);
            }
            QTabBar::tab {
                background-color: rgba(45, 45, 45, 0.5);
                color: #E0E0E0;
                padding: 8px 16px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                margin-right: 2px;
                font-size: 10pt;
            }
            QTabBar::tab:selected {
                background-color: rgba(139, 92, 246, 0.3);
                color: white;
                font-weight: bold;
                border: 1px solid rgba(255, 255, 255, 0.2);
            }
            QTabBar::tab:hover:!selected {
                background-color: rgba(61, 61, 61, 0.5);
            }
        """)
        
        # RESPONSIVE: Remove fixed minimum width
        self.news_frame.setMinimumWidth(600)
        self.news_frame.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        self.all_news_tab = QWidget()
        self.all_news_tab.setObjectName("allNewsTab")
        self.all_news_tab.setStyleSheet("""
            #allNewsTab {
                background-color: transparent;
            }
        """)
        
        # RESPONSIVE: Set size policy
        self.all_news_tab.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        self.all_news_layout = QVBoxLayout(self.all_news_tab)
        self.all_news_layout.setSpacing(0)
        self.all_news_layout.setContentsMargins(0, 0, 0, 0)

        # Update tree widget styles
        tree_style = """
            QTreeWidget {
                background-color: rgba(38, 38, 38, 0.7);
                alternate-background-color: rgba(48, 48, 48, 0.7);
                color: #E0E0E0;
                border: none;
                border-radius: 4px;
                font-size: 10pt;
            }
            QTreeWidget::item {
                min-height: 28px;
                padding: 2px;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            }
            QTreeWidget::item:selected {
                background-color: rgba(139, 92, 246, 0.4);
                color: white;
            }
            QTreeWidget::item:hover:!selected {
                background-color: rgba(255, 255, 255, 0.05);
            }
            QHeaderView::section {
                background-color: rgba(45, 45, 45, 0.8);
                color: #E0E0E0;
                padding: 6px;
                border: none;
                border-right: 1px solid rgba(255, 255, 255, 0.1);
                font-weight: bold;
            }
        """

        self.news_tree = OverlayedTreeWidget(self.on_all_news_tree_click, parent=self.all_news_tab)
        self.news_tree.setStyleSheet(tree_style)
        self.news_tree.setHeaderLabels(['Timestamp', 'Source', 'RIC', 'Headline'])
        
        # RESPONSIVE: Use Interactive mode for column resizing
        self.news_tree.setColumnWidth(0, 140)
        self.news_tree.setColumnWidth(1, 60)
        self.news_tree.setColumnWidth(2, 180)
        self.news_tree.setColumnWidth(3, 560)
        
        # Keep the scrollbar visible to avoid width changes when items are added
        self.news_tree.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        
        header = self.news_tree.header()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)  # Allow manual column resizing
        header.setStretchLastSection(True)  # Last column stretches to fill space
        header.setDefaultAlignment(Qt.AlignmentFlag.AlignLeft)
        
        self.all_news_layout.addWidget(self.news_tree)
        self.news_frame.addTab(self.all_news_tab, "All News")

        self.top_tab = QWidget()
        self.top_tab.setObjectName("topTab")
        self.top_tab.setStyleSheet("""
            #topTab {
                background-color: transparent;
            }
        """)
        
        # RESPONSIVE: Set size policy
        self.top_tab.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        self.top_layout = QVBoxLayout(self.top_tab)
        self.top_layout.setContentsMargins(0, 0, 0, 0)
        self.top_layout.setSpacing(0)

        self.top_tree = OverlayedTreeWidget(self.on_tree_click, parent=self.top_tab)
        self.top_tree.setStyleSheet(tree_style)
        self.top_tree.setSelectionMode(QTreeWidget.SelectionMode.NoSelection)
        self.top_tree.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.top_tree.setHeaderLabels(['RIC', 'Volume', 'Price Range', 'Price Change', 'Headline'])
        
        # RESPONSIVE: Use Interactive mode for column resizing
        self.top_tree.setColumnWidth(0, 120)
        self.top_tree.setColumnWidth(1, 100)
        self.top_tree.setColumnWidth(2, 140)
        self.top_tree.setColumnWidth(3, 140)
        self.top_tree.setColumnWidth(4, 500)
        
        # Keep scrollbar always visible to prevent splitter width jitter
        self.top_tree.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        
        top_header = self.top_tree.header()
        top_header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)  # Allow manual resizing
        top_header.setStretchLastSection(True)  # Last column stretches
        top_header.setDefaultAlignment(Qt.AlignmentFlag.AlignLeft)
        
        self.top_tree.verticalScrollBar().valueChanged.connect(lambda _: self.update_top_tree_display())
        self.top_layout.addWidget(self.top_tree)
        self.news_frame.addTab(self.top_tab, "Top Symbols")

        # Disable context menus on the news and top trees
        self.news_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.NoContextMenu)
        self.top_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.NoContextMenu)

    @profile_function
    def show_news_context_menu(self, position):
        item = self.news_tree.itemAt(position)
        if not item:
            return
        menu = QMenu(self)
        ric = item.text(2)

        copy_action = QAction("Copy Headline", self)
        copy_action.triggered.connect(lambda: self.copy_to_clipboard_from_news(item))
        menu.addAction(copy_action)

        view_action = QAction("View Details", self)
        view_action.triggered.connect(lambda: self.on_all_news_tree_click(item, Qt.MouseButton.LeftButton))
        menu.addAction(view_action)

        menu.addSeparator()

        if self.trading_mode_enabled:
            trade_action = QAction(f"Trade {ric}", self)
            trade_action.triggered.connect(lambda: self.on_all_news_tree_click(item, Qt.MouseButton.LeftButton))
            menu.addAction(trade_action)
            menu.addSeparator()

        chart_action = QAction(f"Add {ric} to Chart", self)
        chart_action.triggered.connect(lambda: self.add_symbol_to_chart(ric))
        menu.addAction(chart_action)

        menu.exec(self.news_tree.mapToGlobal(position))

    @profile_function
    def show_top_context_menu(self, position):
        item = self.top_tree.itemAt(position)
        if not item:
            return
        menu = QMenu(self)
        ric = item.text(0)

        copy_action = QAction("Copy Information", self)
        copy_action.triggered.connect(lambda: self.copy_to_clipboard_from_top(ric))
        menu.addAction(copy_action)

        view_action = QAction("View Details", self)
        view_action.triggered.connect(lambda: self.on_tree_click(item, Qt.MouseButton.LeftButton))
        menu.addAction(view_action)
        menu.addSeparator()

        if self.trading_mode_enabled:
            trade_action = QAction(f"Trade {ric}", self)
            trade_action.triggered.connect(lambda: self.on_tree_click(item, Qt.MouseButton.LeftButton))
            menu.addAction(trade_action)
            menu.addSeparator()

        chart_action = QAction(f"Focus {ric} on Chart", self)
        chart_action.triggered.connect(lambda: self.focus_symbol_on_chart(ric))
        menu.addAction(chart_action)

        menu.exec(self.top_tree.mapToGlobal(position))

    @profile_function
    def copy_to_clipboard_from_news(self, item):
        timestamp = item.text(0)
        source = item.text(1)
        ric = item.text(2)
        headline = item.text(3)
        self.copy_to_clipboard(ric, timestamp, headline, source)
        logging.info(f"[CLIPBOARD] Copied headline for {ric}")

    @profile_function
    def add_symbol_to_chart(self, ric):
        cleaned_rics = self.clean_ric(ric)
        if cleaned_rics:
            symbol = cleaned_rics[0]
            if symbol not in self.subscribed_symbols:
                asyncio.create_task(self.subscribe_to_symbol(symbol))
                self.show_notification(f"Added {symbol} to chart", "success", 1500)
            else:
                self.show_notification(f"{symbol} already on chart", "info", 1500)
            self.event_bus.publish(Event('CHART_UPDATE', None))
            self.focus_symbol_on_chart(symbol)
        else:
            self.show_notification(f"Could not add symbol to chart", "error", 1500)

    @profile_function
    def focus_symbol_on_chart(self, symbol):
        if symbol in self.plot_curves:
            pen = pg.mkPen(color=self.colors.get(symbol, "#FFFFFF"), width=3)
            self.plot_curves[symbol].setPen(pen)
            self.plot_curves[symbol].setZValue(2)
            for s, curve in self.plot_curves.items():
                if s != symbol:
                    pen = pg.mkPen(color=self.colors[s], width=1.5)
                    curve.setPen(pen)
                    curve.setZValue(0)
            if symbol in self.text_items:
                text_item = self.text_items[symbol]
                font = QFont()
                font.setBold(True)
                font.setPointSize(12)
                text_item.setFont(font)
                text_item.setZValue(3)
                for s, item in self.text_items.items():
                    if s != symbol:
                        font = QFont()
                        font.setPointSize(10)
                        item.setFont(font)
                        item.setZValue(0)
            logging.debug(f"[CHART FOCUS] Focused {symbol} on chart at {datetime.utcnow()}")
            self.show_notification(f"Focused {symbol} on chart", "info", 1500)
        else:
            self.show_notification(f"{symbol} not found on chart", "warning", 1500)

    @profile_function
    def reset_chart_highlight(self):
        for symbol, curve in self.plot_curves.items():
            pen = pg.mkPen(color=self.colors[symbol], width=1.5)
            curve.setPen(pen)
            curve.setZValue(0)
        for symbol, text_item in self.text_items.items():
            font = QFont()
            font.setPointSize(10)
            text_item.setFont(font)
            text_item.setZValue(0)

    @profile_function
    def setup_control_panel(self):
        self.control_panel = CardFrame()
        
        # RESPONSIVE: Remove fixed maximum height, set minimum
        self.control_panel.setMinimumHeight(70)
        self.control_panel.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        self.control_layout = QHBoxLayout()
        self.control_layout.setContentsMargins(0, 0, 0, 0)
        self.control_layout.setSpacing(16)

        self.alarms_layout = QHBoxLayout()
        self.alarms_layout.setSpacing(8)

        alarm_label = QLabel("Alarms:")
        alarm_label.setFont(self.app_font_bold)
        self.alarms_layout.addWidget(alarm_label)

        hi_alarm_label = QLabel("High:")
        self.alarms_layout.addWidget(hi_alarm_label)

        self.hi_alarm_entry = ModernDoubleSpinBox(min_value=0.0, max_value=100.0, step=0.1, default_value=0.5, decimals=1)
        self.hi_alarm_entry.setToolTip("Trigger alarm when gain exceeds this percentage")
        self.alarms_layout.addWidget(self.hi_alarm_entry)

        low_alarm_label = QLabel("Low:")
        self.alarms_layout.addWidget(low_alarm_label)

        self.low_alarm_entry = ModernDoubleSpinBox(min_value=-100.0, max_value=0.0, step=0.1, default_value=-0.5, decimals=1)
        self.low_alarm_entry.setToolTip("Trigger alarm when loss exceeds this percentage")
        self.alarms_layout.addWidget(self.low_alarm_entry)

        self.update_alarms_button = ModernButton("Update")
        self.update_alarms_button.setToolTip("Update alarm thresholds")
        self.update_alarms_button.clicked.connect(self.update_alarms)
        self.alarms_layout.addWidget(self.update_alarms_button)

        self.mute_layout = QHBoxLayout()
        self.mute_layout.setSpacing(4)

        self.mute_selected_button = ModernButton("Mute Selected")
        self.mute_selected_button.setCheckable(True)
        self.mute_selected_button.setFixedWidth(90)
        self.mute_selected_button.clicked.connect(self.toggle_mute_selected)
        self.mute_layout.addWidget(self.mute_selected_button)

        self.mute_all_button = ModernButton("Mute All")
        self.mute_all_button.setFixedWidth(80)
        self.mute_all_button.clicked.connect(self.mute_all)
        self.mute_layout.addWidget(self.mute_all_button)

        self.unmute_all_button = ModernButton("Unmute All")
        self.unmute_all_button.setFixedWidth(90)
        self.unmute_all_button.clicked.connect(self.unmute_all)
        self.mute_layout.addWidget(self.unmute_all_button)

        self.alarms_layout.addLayout(self.mute_layout)

        self.control_layout.addLayout(self.alarms_layout)

        separator1 = QFrame()
        separator1.setFrameShape(QFrame.Shape.VLine)
        separator1.setFrameShadow(QFrame.Shadow.Sunken)
        separator1.setStyleSheet("QFrame { background-color: rgba(255, 255, 255, 0.1); }")
        self.control_layout.addWidget(separator1)

        self.ticker_layout = QHBoxLayout()
        self.ticker_layout.setSpacing(8)

        ticker_label = QLabel("Add Symbol:")
        ticker_label.setFont(self.app_font_bold)
        self.ticker_layout.addWidget(ticker_label)

        self.new_ticker_entry = ModernLineEdit(placeholder_text="Enter Ticker")
        self.new_ticker_entry.setToolTip("Enter a ticker symbol to subscribe")
        self.new_ticker_entry.returnPressed.connect(self.handle_ticker_enter)
        self.ticker_layout.addWidget(self.new_ticker_entry)

        self.add_ticker_button = ModernButton("Subscribe")
        self.add_ticker_button.setToolTip("Subscribe to symbol updates")
        self.add_ticker_button.clicked.connect(lambda: asyncio.create_task(self.subscribe_to_symbol(self.new_ticker_entry.text().strip().upper())))
        self.ticker_layout.addWidget(self.add_ticker_button)

        self.control_layout.addLayout(self.ticker_layout)

        separator2 = QFrame()
        separator2.setFrameShape(QFrame.Shape.VLine)
        separator2.setFrameShadow(QFrame.Shadow.Sunken)
        separator2.setStyleSheet("QFrame { background-color: rgba(255, 255, 255, 0.1); }")
        self.control_layout.addWidget(separator2)

        self.trading_layout = QHBoxLayout()
        self.trading_layout.setSpacing(8)

        trading_label = QLabel("Trading:")
        trading_label.setFont(self.app_font_bold)
        self.trading_layout.addWidget(trading_label)

        amount_label = QLabel("Amount:")
        self.trading_layout.addWidget(amount_label)

        self.investment_amount_entry = ModernLineEdit(placeholder_text="Investment Amt")
        self.investment_amount_entry.setText("1000")
        self.trading_layout.addWidget(self.investment_amount_entry)

        slippage_label = QLabel("Slippage:")
        self.trading_layout.addWidget(slippage_label)

        self.slippage_percentage_entry = ModernLineEdit(placeholder_text="Slippage %")
        self.slippage_percentage_entry.setText("1.0")
        self.trading_layout.addWidget(self.slippage_percentage_entry)

        self.trading_mode_button = ModernButton("Enable Trading Mode")
        self.trading_mode_button.clicked.connect(self.toggle_trading_mode)
        self.trading_layout.addWidget(self.trading_mode_button)

        self.control_layout.addLayout(self.trading_layout)

        self.axis_toggle_layout = QHBoxLayout()
        self.axis_toggle_layout.setSpacing(8)

        self.toggle_y_axis_button = ModernButton("Show Cumulative Vol×Price")
        self.toggle_y_axis_button.setCheckable(True)
        self.toggle_y_axis_button.clicked.connect(self.toggle_y_axis_mode)
        self.axis_toggle_layout.addWidget(self.toggle_y_axis_button)

        self.toggle_history_button = ModernButton("1-Min Window")
        self.toggle_history_button.setCheckable(True)
        self.toggle_history_button.clicked.connect(self.toggle_history_mode)
        self.axis_toggle_layout.addWidget(self.toggle_history_button)

        self.control_layout.addLayout(self.axis_toggle_layout)

        separator3 = QFrame()
        separator3.setFrameShape(QFrame.Shape.VLine)
        separator3.setFrameShadow(QFrame.Shadow.Sunken)
        separator3.setStyleSheet("QFrame { background-color: rgba(255, 255, 255, 0.1); }")
        self.control_layout.addWidget(separator3)

        self.control_panel.add_layout(self.control_layout)
        self.main_layout.addWidget(self.control_panel)
        self.update_mute_button_state()

    @profile_function
    def toggle_trading_mode(self, checked=False):
        self.trading_mode_enabled = not self.trading_mode_enabled
        if self.trading_mode_enabled:
            self.trading_mode_button.setText("Trading Mode ON")
            self.trading_mode_button.setStyleSheet(f"""
                QPushButton {{
                    background-color: rgba(16, 185, 129, 0.3);
                    color: white;
                    border: 1px solid rgba(16, 185, 129, 0.5);
                    border-radius: 4px;
                    padding: 6px 12px;
                    font-weight: 500;
                    font-size: 12px;
                    backdrop-filter: blur(10px);
                    -webkit-backdrop-filter: blur(10px);
                }}
                QPushButton:hover {{
                    background-color: rgba(16, 185, 129, 0.4);
                    border: 1px solid rgba(16, 185, 129, 0.6);
                }}
                QPushButton:pressed {{
                    background-color: rgba(16, 185, 129, 0.5);
                }}
            """)
            self.debug_label.setText("Trading mode enabled. Click items to trigger actions.")
            self.show_notification("Trading mode enabled", "success", 2000)
        else:
            self.trading_mode_button.setText("Enable Trading Mode")
            # Revert to default style
            self.trading_mode_button.setStyleSheet("")
            self.debug_label.setText("Trading mode disabled")
            self.show_notification("Trading mode disabled", "info", 2000)

    @profile_function
    def toggle_y_axis_mode(self, checked=False):
        if self.y_axis_mode == 'gain':
            self.y_axis_mode = 'volume_price'
            self.plot_widget.setLabel('right', 'Cumulative Volume × Price', color='#CCCCCC')
            self.toggle_y_axis_button.setText('Show % Gain')
        else:
            self.y_axis_mode = 'gain'
            self.plot_widget.setLabel('right', 'Cumulative Percent Gain', color='#CCCCCC')
            self.toggle_y_axis_button.setText('Show Cumulative Vol×Price')
        self.rerank_top_symbols()
        self.event_bus.publish(Event('CHART_UPDATE', None))

    @profile_function
    def toggle_history_mode(self, checked=False):
        if self.chart_history_seconds == DEFAULT_CHART_HISTORY_SECONDS:
            self.chart_history_seconds = SHORT_CHART_HISTORY_SECONDS
            self.toggle_history_button.setText('Full History')
        else:
            self.chart_history_seconds = DEFAULT_CHART_HISTORY_SECONDS
            self.toggle_history_button.setText('1-Min Window')

        # use selected window
        new_max = SHORT_MAX_POINTS if self.chart_history_seconds == SHORT_CHART_HISTORY_SECONDS else DEFAULT_CHART_HISTORY_SECONDS
        for buf in self.price_data.values():
            buf.set_maxlen(new_max)
        for buf in self.volume_price_data.values():
            buf.set_maxlen(new_max)

        self.cleanup_old_data()
        self.event_bus.publish(Event('CHART_UPDATE', None))

    @profile_function
    def rerank_top_symbols(self):
        score_dict = {}
        for symbol, idx in self.symbol_index_map.items():
            if self.y_axis_mode == 'gain':
                base_price = self.base_prices[idx]
                latest_price = self.latest_prices[idx]
                score = ((latest_price - base_price) / base_price) * 100 if base_price else 0
            else:
                score = self.volume_price_totals.get(symbol, 0)
            score_dict[symbol] = score

        self.top_tracker = TopTracker(max_items=MAX_CHART_LINES)
        for symbol, score in score_dict.items():
            self.top_tracker.update(symbol, score)
        self.update_top_tree(force=True)

    @profile_function
    def toggle_mute_selected(self, checked=False):
        symbol = self.selected_ric
        if not symbol:
            return
        if symbol in self.muted_symbols:
            self.muted_symbols.remove(symbol)
        else:
            self.muted_symbols.add(symbol)
        self.musical_alarm_system.audio_player.clear_queue()
        self.audio_player.clear_queue()
        self.update_mute_button_state()

    @profile_function
    def mute_all(self, checked=False):
        self.muted_symbols = set(self.subscribed_symbols)
        self.musical_alarm_system.audio_player.clear_queue()
        self.audio_player.clear_queue()
        self.update_mute_button_state()

    @profile_function
    def unmute_all(self, checked=False):
        self.muted_symbols.clear()
        self.musical_alarm_system.audio_player.clear_queue()
        self.audio_player.clear_queue()
        self.update_mute_button_state()

    @profile_function
    def update_mute_button_state(self):
        if not hasattr(self, 'mute_selected_button'):
            return
        if self.selected_ric and self.selected_ric in self.muted_symbols:
            self.mute_selected_button.setChecked(True)
            self.mute_selected_button.setText("Unmute Selected")
        else:
            self.mute_selected_button.setChecked(False)
            self.mute_selected_button.setText("Mute Selected")

    @profile_function
    def trigger_hover_animation(self, widget, duration=200):
        enter = QEvent(QEvent.Type.Enter)
        leave = QEvent(QEvent.Type.Leave)
        QApplication.sendEvent(widget, enter)
        QTimer.singleShot(duration, lambda: QApplication.sendEvent(widget, leave))

    @profile_function
    def toggle_mute_all_hotkey(self):
        if self.subscribed_symbols and self.muted_symbols == set(self.subscribed_symbols):
            self.unmute_all()
            if hasattr(self, 'unmute_all_button'):
                self.trigger_hover_animation(self.unmute_all_button)
        else:
            self.mute_all()
            if hasattr(self, 'mute_all_button'):
                self.trigger_hover_animation(self.mute_all_button)

    @profile_function
    def toggle_y_axis_mode_hotkey(self):
        self.toggle_y_axis_mode()
        if hasattr(self, 'toggle_y_axis_button'):
            self.trigger_hover_animation(self.toggle_y_axis_button)

    @profile_function
    def focus_ticker_entry(self):
        if hasattr(self, 'new_ticker_entry'):
            self.new_ticker_entry.setFocus()
            self.trigger_hover_animation(self.new_ticker_entry)

    @profile_function
    def handle_ticker_enter(self):
        symbol = self.new_ticker_entry.text().strip().upper()
        self.new_ticker_entry.clear()
        self.new_ticker_entry.clearFocus()
        if symbol:
            asyncio.create_task(self.subscribe_to_symbol(symbol))
            if hasattr(self, 'add_ticker_button'):
                self.trigger_hover_animation(self.add_ticker_button)

    @profile_function
    def delete_selected_item(self):
        symbol = getattr(self, 'selected_ric', None)
        if not symbol:
            return

        if self.news_frame.currentIndex() == 0:
            item = getattr(self, 'last_clicked_news_item', None)
            if item:
                root = self.news_tree.invisibleRootItem()
                idx = root.indexOfChild(item)
                if idx != -1:
                    root.takeChild(idx)
        else:
            item = getattr(self, 'last_clicked_top_item', None)
            if item:
                root = self.top_tree.invisibleRootItem()
                idx = root.indexOfChild(item)
                if idx != -1:
                    root.takeTopLevelItem(idx)

        asyncio.create_task(self.unsubscribe_and_remove_data(symbol))
        self.selected_ric = None
        self.last_clicked_top_item = None
        self.last_clicked_news_item = None
        QTimer.singleShot(0, lambda: self.update_top_tree_display(force=True))

    @profile_function
    def switch_tab(self, direction):
        idx = self.news_frame.currentIndex()
        count = self.news_frame.count()
        self.news_frame.setCurrentIndex((idx + direction) % count)
        self.trigger_hover_animation(self.news_frame.tabBar())

    @profile_function
    def scroll_active_tree_top(self):
        tree = self.news_tree if self.news_frame.currentIndex() == 0 else self.top_tree
        tree.scrollToTop()
        self.trigger_hover_animation(tree)

    @profile_function
    def scroll_active_tree_bottom(self):
        tree = self.news_tree if self.news_frame.currentIndex() == 0 else self.top_tree
        tree.scrollToBottom()
        self.trigger_hover_animation(tree)

    @profile_function
    def scroll_active_tree_page_up(self):
        tree = self.news_tree if self.news_frame.currentIndex() == 0 else self.top_tree
        scrollbar = tree.verticalScrollBar()
        scrollbar.setValue(scrollbar.value() - scrollbar.pageStep())
        self.trigger_hover_animation(tree)

    @profile_function
    def scroll_active_tree_page_down(self):
        tree = self.news_tree if self.news_frame.currentIndex() == 0 else self.top_tree
        scrollbar = tree.verticalScrollBar()
        scrollbar.setValue(scrollbar.value() + scrollbar.pageStep())
        self.trigger_hover_animation(tree)

    @profile_function
    def select_active_tree_top_item(self):
        tree = self.news_tree if self.news_frame.currentIndex() == 0 else self.top_tree
        if tree.topLevelItemCount() == 0:
            return
        item = tree.topLevelItem(0)
        tree.scrollToItem(item)
        if tree is self.news_tree:
            self.on_all_news_tree_click(item, Qt.MouseButton.LeftButton)
        else:
            self.on_tree_click(item, Qt.MouseButton.LeftButton)
        self.trigger_hover_animation(tree)

    @profile_function
    def select_active_tree_bottom_item(self):
        tree = self.news_tree if self.news_frame.currentIndex() == 0 else self.top_tree
        count = tree.topLevelItemCount()
        if count == 0:
            return
        item = tree.topLevelItem(count - 1)
        tree.scrollToItem(item)
        if tree is self.news_tree:
            self.on_all_news_tree_click(item, Qt.MouseButton.LeftButton)
        else:
            self.on_tree_click(item, Qt.MouseButton.LeftButton)
        self.trigger_hover_animation(tree)

    @profile_function
    def setup_data_structures(self):
        self.polygon_websocket = None
        self.news_websocket = None
        self.price_data = {}
        self.volume_price_data = {}
        self.volume_price_totals = {}
        self.news_buffer = LockFreeCircularBuffer(100000)
        self.price_buffer = LockFreeCircularBuffer(100000)
        shm_latest = shared_memory.SharedMemory(create=True, size=8 * MAX_SYMBOLS)
        self.shared_latest = shm_latest
        self.latest_prices = np.ndarray((MAX_SYMBOLS,), dtype='f8', buffer=shm_latest.buf)
        self.latest_prices.fill(0)
        self.colors = {}

        # OPTIMIZED: Use lazy color generation instead of pre-generating 1500 colors
        self.color_pool = LazyColorPool()

        self.subscribed_symbols = set()
        self.muted_symbols = set()
        shm_base = shared_memory.SharedMemory(create=True, size=8 * MAX_SYMBOLS)
        self.shared_base = shm_base
        self.base_prices = np.ndarray((MAX_SYMBOLS,), dtype='f8', buffer=shm_base.buf)
        self.base_prices.fill(0)
        self.symbol_data = np.zeros(MAX_SYMBOLS, dtype=[
            ('symbol', 'U20'),
            ('min', 'f8'),
            ('max', 'f8'),
            ('last', 'f8'),
            ('volume', 'i8'),
            ('timestamp', 'datetime64[ns]'),
            ('max_pos_percent', 'f8'),
            ('max_neg_percent', 'f8')
        ])

        self.top_data = {}
        self.top_tracker = TopTracker(max_items=MAX_CHART_LINES)
        self.last_update_times = {}
        self.log_interval = 1
        self.last_log_time = 0
        self.top_tree_data = {}
        self.is_dark_mode = True
        self.hi_alarm = 0.5
        self.low_alarm = -0.5
        self.alarm_cooldowns = {}
        self.alarm_cooldown_period = 1

        self.executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        self.musical_alarm_system = MusicalAlarmSystem()
        self.audio_player = NonBlockingAudio(
            backlog_check_interval=1,
            max_queue_size=100,
            allow_polyphony=False
        )
        self.ric_to_clipboard_text = {}
        self.ric_clipboard_queue = deque(maxlen=10000)
        self.top_tree_items = {}

        self.script_start_time = make_timestamp_naive_utc(datetime.now(pytz.UTC))
        logging.info(f"Script start time recorded as {self.script_start_time}")

        self.recent_headlines_per_ric = defaultdict(lambda: deque(maxlen=5))
        self.selected_ric = None
        self.last_clicked_top_item = None
        self.last_clicked_news_item = None
        self.news_queue = Queue()
        self.price_queue = Queue()
        self.symbol_index_map = {}
        self.available_indices = []
        self.data_lock = threading.RLock()
        self.pending_news_items = []
        self.http_session = None

    @profile_function
    def acquire_color(self) -> str:
        """Acquire a color from the lazy pool."""
        return self.color_pool.acquire_color()

    @profile_function
    def release_color(self, color: str):
        """Release a color back to the pool."""
        self.color_pool.release_color(color)

    @profile_function
    def setup_event_handlers(self):
        self.event_bus.event_occurred.connect(self.event_bus.process_event)
        # Register chart update handler so published events trigger the
        # corresponding callback. Without this subscription the chart
        # never receives data to plot.
        self.event_bus.subscribe('CHART_UPDATE', self.handle_chart_update)

    @profile_function
    def setup_async_tasks(self):
        self.loop = asyncio.get_event_loop()
        self.loop.create_task(self.ensure_http_session())
        self.loop.create_task(self.connect_to_server())
        self.loop.create_task(self.connect_to_polygon())
        self.loop.create_task(self.chart_update_loop())
        self.loop.create_task(self.update_loop())
        self.loop.create_task(self.perform_cleanup())
        self.loop.create_task(self.process_news_batch())
        self.loop.create_task(self.process_price_batch())

    @profile_function
    async def ensure_http_session(self):
        if not getattr(self, 'http_session', None) or self.http_session.closed:
            connector = aiohttp.TCPConnector(limit=100)
            self.http_session = aiohttp.ClientSession(connector=connector)
        return self.http_session

    ########################################################
    # Connection logic
    ########################################################
    @profile_function
    async def connect_to_server(self):
        while True:
            try:
                async with websockets.connect(NEWS_WS_URI) as websocket:
                    self.news_websocket = websocket
                    self.update_connection_status(True)
                    await self.receive_messages(websocket)
            except Exception as e:
                self.rate_limited_log('ERROR', f"Connection error: {e}")
                self.update_connection_status(False)
                await asyncio.sleep(5)

    @profile_function
    async def receive_messages(self, websocket):
        while True:
            try:
                message = await websocket.recv()
                if isinstance(message, str):
                    data = orjson.loads(message.encode('utf-8'))
                elif isinstance(message, bytes):
                    data = orjson.loads(message)
                else:
                    self.rate_limited_log('ERROR', f"Unsupported message type: {type(message)}")
                    continue
                if isinstance(data, dict):
                    await self.news_queue.put(data)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            await self.news_queue.put(item)
                        else:
                            logging.error(f"Non-dict item in data list: {item} ({type(item)})")
                else:
                    logging.error(f"Received unexpected data type: {type(data)}. Data: {data}")
            except websockets.exceptions.ConnectionClosed:
                self.rate_limited_log('ERROR', "News server connection closed")
                break
            except orjson.JSONDecodeError:
                self.rate_limited_log('ERROR', f"Failed to decode JSON from news server: {message}")
            except Exception as e:
                self.rate_limited_log('ERROR', f"Error receiving news messages: {e}")


    @profile_function
    def add_news_item_to_ui(self, item):
        logging.debug(f"[NEWS ITEM QUEUE] Adding item to pending_news_items -> Headline: {item.text(3)}")
        self.pending_news_items.append(item)

    @profile_function
    async def process_news_batch(self):
        while True:
            batch = []
            try:
                item = await asyncio.wait_for(self.news_queue.get(), timeout=BATCH_INTERVAL)
                batch.append(item)
                while len(batch) < 1000:
                    batch.append(self.news_queue.get_nowait())
            except (asyncio.TimeoutError, QueueEmpty):
                pass
            if batch:
                clean_items = await self.loop.run_in_executor(
                    self.process_pool,
                    process_news_batch_worker,
                    batch,
                )
                self._process_news_items_internal(clean_items)

    @profile_function
    def _process_news_items_internal(self, news_items):
        for idx, itm in enumerate(news_items):
            if not isinstance(itm, dict):
                logging.error(f"Non-dict item at index {idx} in news_items: {itm} ({type(itm)})")

        for news_item in news_items:
            if not isinstance(news_item, dict):
                continue
            timestamp = news_item.get('timestamp', '')
            source = news_item.get('source', '')
            ric = news_item.get('ric', '')
            headline = news_item.get('headline', '')

            logging.debug(f"[NEWS ARRIVAL] Received news item. Time: {timestamp}, RIC: {ric}, Headline: {headline}")
            cleaned_rics = self.clean_ric(ric)
            if not cleaned_rics:
                self.rate_limited_log('INFO', f"No valid RICs after filtering for news item: {ric}")
                continue

            for sym in cleaned_rics:
                if sym not in self.colors:
                    self.colors[sym] = self.acquire_color()
            primary_symbol = cleaned_rics[0]
            for cleaned_ric in cleaned_rics:
                if cleaned_ric not in self.subscribed_symbols:
                    asyncio.run_coroutine_threadsafe(self.subscribe_to_symbol(cleaned_ric), self.loop)
                asyncio.run_coroutine_threadsafe(self.ensure_symbol_initialized(cleaned_ric), self.loop)

            QItem = QTreeWidgetItem([timestamp, source, ric, headline])
            QItem.setData(2, Qt.ItemDataRole.UserRole, primary_symbol)
            icon_symbols = cleaned_rics[:3]
            icon = self.create_ticker_icon(icon_symbols)
            QItem.setIcon(2, icon)
            QItem.setToolTip(3, headline)
            self.add_news_item_to_ui(QItem)

            self.add_recent_headline(timestamp, source, ric, headline)
            clipboard_text = self.generate_clipboard_text(timestamp, source, ric, headline)
            self.ric_to_clipboard_text[ric] = clipboard_text
            self.ric_clipboard_queue.append(ric)

            for cleaned_ric in cleaned_rics:
                self.symbol_mapping.setdefault(cleaned_ric, set()).add(ric)
                self.ric_to_clipboard_text[cleaned_ric] = clipboard_text
                self.ric_clipboard_queue.append(cleaned_ric)
                self.add_recent_headline(timestamp, source, cleaned_ric, headline)
                self.event_bus.publish(Event('CHART_UPDATE', None))

            for single_ric in ric.split():
                clean_ric = single_ric.strip('()')
                if clean_ric not in self.top_data:
                    self.top_data[clean_ric] = []
                self.top_data[clean_ric].append({
                    'timestamp': timestamp,
                    'headline': headline
                })

        self.cleanup_news_tree()
        if news_items:
            latest_news = news_items[-1]
            if isinstance(latest_news, dict):
                debug_text = f"Latest news: {latest_news.get('ric', 'N/A')} - Received: {make_timestamp_naive_utc(datetime.now(pytz.UTC))}, Original: {latest_news.get('timestamp', 'N/A')}"
            else:
                logging.error(f"latest_news is not a dict: {type(latest_news)}: {latest_news}")
                debug_text = "Latest news: Unable to process non-dict latest news item."
            self.debug_label.setText(debug_text)

    @profile_function
    def add_recent_headline(self, timestamp, source, ric, headline):
        formatted_headline = self.generate_clipboard_text(timestamp, source, ric, headline)
        self.recent_headlines_per_ric[ric].appendleft(formatted_headline)
        if self.selected_ric == ric:
            QTimer.singleShot(0, lambda ric=ric: self.update_recent_headlines_widget(ric))

    ########################################################
    # Polygon
    ########################################################
    @profile_function
    async def connect_to_polygon(self):
        while True:
            try:
                async with websockets.connect(POLYGON_WS_URI, ping_interval=None) as websocket:
                    self.polygon_websocket = websocket
                    await self.authenticate_polygon()
                    self.update_connection_status(True)
                    await self.receive_polygon_messages()
            except Exception as e:
                self.rate_limited_log('ERROR', f"Error in Polygon connection: {e}")
                self.update_connection_status(False)
                await asyncio.sleep(1)

    @profile_function
    async def authenticate_polygon(self):
        auth_message = {"action": "auth", "params": POLYGON_API_KEY}
        auth_bytes = orjson.dumps(auth_message)
        await self.polygon_websocket.send(auth_bytes)
        auth_response = await self.polygon_websocket.recv()
        self.rate_limited_log('INFO', f"Polygon authentication response: {auth_response}")
        if isinstance(auth_response, str):
            auth_data = orjson.loads(auth_response.encode('utf-8'))
        else:
            auth_data = orjson.loads(auth_response)
        if isinstance(auth_data, list) and auth_data and auth_data[0].get('status') != 'connected':
            raise Exception(f"Polygon authentication failed: {auth_response}")

    @profile_function
    async def subscribe_to_symbol(self, symbol):
        self.rate_limited_log('INFO', f"Attempting to subscribe to symbol: {symbol}")
        if symbol and symbol not in self.subscribed_symbols:
            if len(self.subscribed_symbols) >= MAX_SYMBOLS:
                self.rate_limited_log('WARNING', f"Maximum symbols ({MAX_SYMBOLS}) reached. Cannot subscribe to {symbol}.")
                self.show_notification(f"Max symbols reached, cannot add {symbol}", "warning", 2000)
                return
            self.subscribed_symbols.add(symbol)
            self.price_data[symbol] = PriceCircularBuffer(DEFAULT_CHART_HISTORY_SECONDS)
            self.volume_price_data[symbol] = PriceCircularBuffer(DEFAULT_CHART_HISTORY_SECONDS)
            try:
                await self.fetch_last_trade(symbol)
                await self.send_subscription_message(symbol)
                self.rate_limited_log('INFO', f"Successfully subscribed to symbol: {symbol}")
                self.show_notification(f"Subscribed to {symbol}", "success", 1500)
            except Exception as e:
                self.rate_limited_log('ERROR', f"Failed to subscribe to symbol {symbol}: {e}")
                self.subscribed_symbols.remove(symbol)
                del self.price_data[symbol]
                del self.volume_price_data[symbol]
                if symbol in self.volume_price_totals:
                    del self.volume_price_totals[symbol]
                self.show_notification(f"Failed to subscribe to {symbol}", "error", 2000)
        else:
            self.rate_limited_log('INFO', f"Symbol {symbol} already subscribed or invalid")
            if symbol in self.subscribed_symbols:
                self.show_notification(f"{symbol} already subscribed", "info", 1500)

    @profile_function
    async def fetch_last_trade(self, symbol):
        url = f"{POLYGON_REST_URL}/v2/last/trade/{symbol}?apiKey={POLYGON_API_KEY}"
        logging.info(f"Fetching last trade for {symbol} from {url}")
        session = await self.ensure_http_session()
        try:
            async with session.get(url) as response:
                if response.status == 200:
                        text = await response.text()
                        data = orjson.loads(text.encode('utf-8'))
                        if data.get('status') == 'OK' and 'results' in data:
                            result = data['results']
                            price = result.get('p')
                            volume = result.get('s', 0)
                            raw_timestamp = result.get('t')
                            if price is None or raw_timestamp is None:
                                logging.error(f"Missing price or timestamp in last trade data for {symbol}. Data: {result}")
                                return
                            timestamp = datetime.utcfromtimestamp(raw_timestamp/1_000_000_000).replace(tzinfo=pytz.UTC)
                            timestamp = make_timestamp_naive_utc(timestamp)
                            if timestamp < self.script_start_time:
                                timestamp = self.script_start_time
                            self.update_symbol_data(symbol, price, volume, timestamp)
                            logging.info(f"Fetched last trade for {symbol}: {price} at {timestamp}, volume: {volume}")
                            self.event_bus.publish(Event('CHART_UPDATE', None))
                        else:
                            logging.error(f"No trade data found for {symbol}. Response: {data}")
                elif response.status == 404:
                    logging.error(f"Symbol not found: {symbol}")
                else:
                    logging.error(f"Failed to fetch last trade for {symbol}: HTTP {response.status}")
        except aiohttp.ClientError as e:
            logging.error(f"Network error fetching last trade for {symbol}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error fetching last trade for {symbol}: {e}")

    @profile_function
    def update_symbol_data(self, symbol, price, volume, timestamp):
        naive_timestamp = make_timestamp_naive_utc(timestamp)
        logging.debug(
            f"[DATA UPDATE] update_symbol_data for {symbol} with price={price}, volume={volume}, time={naive_timestamp}"
        )

        with self.data_lock:
            if symbol not in self.subscribed_symbols or symbol not in self.price_data:
                logging.debug(f"Ignoring update for unsubscribed or unknown symbol {symbol}")
                return

            if symbol not in self.symbol_index_map:
                if self.available_indices:
                    idx = self.available_indices.pop(0)
                else:
                    used = self.symbol_index_map.values()
                    idx = (max(used) + 1) if used else 0  # safer fallback
                if idx >= MAX_SYMBOLS:
                    self.rate_limited_log('ERROR', f'Max symbols {MAX_SYMBOLS} reached')
                    return
                self.symbol_index_map[symbol] = idx
                logging.info(f"Added symbol {symbol} to symbol_index_map at index {idx}")
                self.base_prices[idx] = price
                self.latest_prices[idx] = price
                percent_gain = 0.0
                self.symbol_data[idx] = (
                    symbol,
                    price,
                    price,
                    price,
                    volume,
                    np.datetime64(naive_timestamp),
                    percent_gain,
                    percent_gain,
                )
                if symbol not in self.volume_price_data:
                    self.volume_price_data[symbol] = PriceCircularBuffer(DEFAULT_CHART_HISTORY_SECONDS)
                self.volume_price_totals[symbol] = self.volume_price_totals.get(symbol, 0) + volume * price
                self.price_data[symbol].append(naive_timestamp, percent_gain)
                self.volume_price_data[symbol].append(naive_timestamp, self.volume_price_totals[symbol])
                self.rate_limited_log(
                    'DEBUG', f"Appending to price_data[{symbol}]: timestamp={naive_timestamp}, gain={percent_gain}"
                )
            else:
                idx = self.symbol_index_map[symbol]
                self.latest_prices[idx] = price
                self.symbol_data['min'][idx] = np.minimum(self.symbol_data['min'][idx], price)
                self.symbol_data['max'][idx] = np.maximum(self.symbol_data['max'][idx], price)
                self.symbol_data['last'][idx] = price
                self.symbol_data['volume'][idx] += volume
                self.symbol_data['timestamp'][idx] = np.datetime64(naive_timestamp)

                base_price = self.base_prices[idx]
                percent_gain = ((price - base_price) / base_price) * 100 if base_price else 0

                self.symbol_data['max_pos_percent'][idx] = np.maximum(
                    self.symbol_data['max_pos_percent'][idx], percent_gain
                )
                self.symbol_data['max_neg_percent'][idx] = np.minimum(
                    self.symbol_data['max_neg_percent'][idx], percent_gain
                )

                self.price_data[symbol].append(naive_timestamp, percent_gain)
                if symbol not in self.volume_price_data:
                    self.volume_price_data[symbol] = PriceCircularBuffer(DEFAULT_CHART_HISTORY_SECONDS)
                self.volume_price_totals[symbol] = self.volume_price_totals.get(symbol, 0) + volume * price
                self.volume_price_data[symbol].append(naive_timestamp, self.volume_price_totals[symbol])
                self.last_update_times[symbol] = np.datetime64(naive_timestamp)
                self.rate_limited_log('DEBUG', f"Symbol data updated for {symbol}")

        # assert after mutation
        self._assert_index_consistency()

    @profile_function
    async def send_subscription_message(self, symbol):
        subscribe_message = {"action": "subscribe", "params": f"T.{symbol}"}
        json_bytes = orjson.dumps(subscribe_message)
        await self.polygon_websocket.send(json_bytes)
        self.rate_limited_log('INFO', f"Subscribed to Polygon feed for: {symbol}")

    @profile_function
    async def send_unsubscribe_message(self, symbol):
        if not self.polygon_websocket or self.polygon_websocket.closed:
            return
        unsubscribe_message = {"action": "unsubscribe", "params": f"T.{symbol}"}
        json_bytes = orjson.dumps(unsubscribe_message)
        await self.polygon_websocket.send(json_bytes)
        self.rate_limited_log('INFO', f"Unsubscribed from Polygon feed for: {symbol}")

    @profile_function
    async def unsubscribe_and_remove_data(self, symbol):
        if symbol in self.subscribed_symbols:
            try:
                await self.send_unsubscribe_message(symbol)
            except Exception as e:
                self.rate_limited_log('ERROR', f"Failed to unsubscribe {symbol}: {e}")
            self.subscribed_symbols.remove(symbol)

        self.muted_symbols.discard(symbol)

        if symbol in self.top_tracker.symbol_scores:
            try:
                self.top_tracker._remove(symbol)
            except Exception:
                pass

        self.price_data.pop(symbol, None)
        self.volume_price_data.pop(symbol, None)
        self.volume_price_totals.pop(symbol, None)

        with self.data_lock:
            if symbol in self.symbol_index_map:
                idx = self.symbol_index_map.pop(symbol)
                self.symbol_data[idx] = ('', 0.0, 0.0, 0.0, 0, np.datetime64('NaT'), 0.0, 0.0)
                self.base_prices[idx] = 0.0
                self.latest_prices[idx] = 0.0
                self.available_indices.append(idx)

        if symbol in self.plot_curves:
            curve = self.plot_curves.pop(symbol)
            self.plot_widget.removeItem(curve)
        if symbol in self.text_items:
            text_item = self.text_items.pop(symbol)
            self.plot_widget.removeItem(text_item)

        color_hex = self.colors.pop(symbol, None)
        if color_hex:
            self.release_color(color_hex)
        self.top_tree_data.pop(symbol, None)

        if symbol in self.top_tree_items:
            item = self.top_tree_items.pop(symbol)
            idx = self.top_tree.indexOfTopLevelItem(item)
            if idx != -1:
                self.top_tree.takeTopLevelItem(idx)

        # Remove related headlines from the all-news tree
        for i in range(self.news_tree.topLevelItemCount() - 1, -1, -1):
            itm = self.news_tree.topLevelItem(i)
            user_symbol = itm.data(2, Qt.ItemDataRole.UserRole)
            ric_text = itm.text(2)
            if user_symbol == symbol or (ric_text and symbol in ric_text):
                self.news_tree.takeTopLevelItem(i)
        self.pending_news_items = [
            itm for itm in self.pending_news_items
            if itm.data(2, Qt.ItemDataRole.UserRole) != symbol
            and symbol not in itm.text(2)
        ]

        self.recent_headlines_per_ric.pop(symbol, None)
        QTimer.singleShot(0, lambda symbol=symbol: self.update_recent_headlines_widget(symbol))

        # post-check
        self._assert_index_consistency()

        self.show_notification(f"Removed {symbol}", "info", 1500)

    @profile_function
    async def receive_polygon_messages(self):
        while True:
            try:
                message = await self.polygon_websocket.recv()
                if isinstance(message, str):
                    data = orjson.loads(message.encode('utf-8'))
                else:
                    data = orjson.loads(message)
                if isinstance(data, list):
                    for item in data:
                        await self.price_queue.put(item)
                elif isinstance(data, dict):
                    await self.price_queue.put(data)
            except websockets.exceptions.ConnectionClosed:
                self.rate_limited_log('ERROR', "Polygon connection closed")
                self.update_connection_status(False)
                break
            except orjson.JSONDecodeError:
                self.rate_limited_log('ERROR', f"Failed to decode JSON from Polygon: {message}")
            except Exception as e:
                self.rate_limited_log('ERROR', f"Error in Polygon connection: {e}")
                self.update_connection_status(False)
                break
        await self.cleanup_websockets()


    @profile_function
    async def process_price_batch(self):
        while True:
            batch = []
            try:
                item = await asyncio.wait_for(self.price_queue.get(), timeout=BATCH_INTERVAL)
                batch.append(item)
                while len(batch) < 1000:
                    batch.append(self.price_queue.get_nowait())
            except (asyncio.TimeoutError, QueueEmpty):
                pass
            if batch:
                updates = await self.loop.run_in_executor(
                    self.process_pool,
                    process_price_batch_worker,
                    batch,
                    self.script_start_time,
                )
                for sym, (price, volume, naive_timestamp) in updates.items():
                    self.update_symbol_data(sym, price, volume, naive_timestamp)
                    if sym not in self.symbol_index_map:
                        continue
                    idx = self.symbol_index_map[sym]
                    base_price = self.base_prices[idx]
                    percent_gain = ((price - base_price) / base_price) * 100 if base_price else 0
                    score = percent_gain if self.y_axis_mode == 'gain' else self.volume_price_totals.get(sym, 0)
                    self.top_tracker.update(sym, score)
                    if percent_gain >= self.hi_alarm:
                        self.musical_alarm_system.play_note(sym, self.muted_symbols)
                        self.play_alarm('high', sym)
                        self.show_notification(f"{sym} crossed high threshold: {percent_gain:.2f}%", "success", 2000)
                    elif percent_gain <= self.low_alarm:
                        self.musical_alarm_system.play_note(sym, self.muted_symbols)
                        self.play_alarm('low', sym)
                        self.show_notification(f"{sym} crossed low threshold: {percent_gain:.2f}%", "warning", 2000)
                QTimer.singleShot(0, self.update_top_tree)

    @profile_function
    def _process_price_updates_internal(self, price_updates):
        valid = []
        for d in price_updates:
            if not isinstance(d, dict):
                continue
            sym = d.get('sym')
            price = d.get('p')
            volume = d.get('s')
            ts = d.get('t')
            if sym and price is not None and volume is not None and isinstance(ts, (int, float)):
                valid.append((sym, price, volume, ts))
        if not valid:
            return

        dtype = [('sym', 'U20'), ('p', 'f8'), ('s', 'f8'), ('t', 'i8')]
        arr = np.array(valid, dtype=dtype)
        unique_syms = np.unique(arr['sym'])
        for sym in unique_syms:
            mask = arr['sym'] == sym
            price = arr['p'][mask][-1]
            volume = arr['s'][mask].sum()
            raw_ts = arr['t'][mask][-1]
            timestamp = datetime.utcfromtimestamp(raw_ts / 1000).replace(tzinfo=pytz.UTC)
            timestamp = make_timestamp_naive_utc(timestamp)
            if timestamp < self.script_start_time:
                timestamp = self.script_start_time
            naive_timestamp = make_timestamp_naive_utc(timestamp)

            self.update_symbol_data(sym, price, volume, naive_timestamp)
            if sym not in self.symbol_index_map:
                continue
            idx = self.symbol_index_map[sym]
            base_price = self.base_prices[idx]
            percent_gain = ((price - base_price) / base_price) * 100 if base_price else 0
            if self.y_axis_mode == 'gain':
                score = percent_gain
            else:
                score = self.volume_price_totals.get(sym, 0)
            self.top_tracker.update(sym, score)
            if percent_gain >= self.hi_alarm:
                self.musical_alarm_system.play_note(sym, self.muted_symbols)
                self.play_alarm('high', sym)
                self.show_notification(f"{sym} crossed high threshold: {percent_gain:.2f}%", "success", 2000)
            elif percent_gain <= self.low_alarm:
                self.musical_alarm_system.play_note(sym, self.muted_symbols)
                self.play_alarm('low', sym)
                self.show_notification(f"{sym} crossed low threshold: {percent_gain:.2f}%", "warning", 2000)
        QTimer.singleShot(0, self.update_top_tree)

    @profile_function
    def update_top_tree(self, force: bool = False):
        top_symbols = self.top_tracker.get_top()
        self.top_tree_data = {}
        for score, symbol in top_symbols:
            price_data = self.find_news_data(symbol)
            idx = self.symbol_index_map[symbol]
            volume = self.symbol_data[idx]['volume']
            price_range = f"{self.symbol_data[idx]['min']:.2f} - {self.symbol_data[idx]['max']:.2f}"
            price_change = self.symbol_data[idx]['last'] - self.base_prices[idx]
            percent_change = (price_change / self.base_prices[idx]) * 100 if self.base_prices[idx] else 0
            headline = price_data['headline'] if price_data else ''
            self.top_tree_data[symbol] = {
                'volume': volume,
                'price_range': price_range,
                'price_change': price_change,
                'percent_change': percent_change,
                'headline': headline
            }
        QTimer.singleShot(0, lambda: self.update_top_tree_display(force))


    @profile_function
    async def check_alarms(self, symbol, percent_gain):
        current_time = time.time()
        last_alarm_time = self.alarm_cooldowns.get(symbol, 0)
        if percent_gain >= self.hi_alarm and current_time - last_alarm_time >= self.alarm_cooldown_period:
            self.play_alarm('high', symbol)
            self.musical_alarm_system.play_note(symbol, self.muted_symbols)
            self.alarm_cooldowns[symbol] = current_time
            await self.update_debug_label(f"High alarm triggered for {symbol}: {percent_gain:.2f}%")
            self.show_notification(f"High Alert: {symbol} at {percent_gain:.2f}%", "success", 2000)
        elif percent_gain <= self.low_alarm and current_time - last_alarm_time >= self.alarm_cooldown_period:
            self.play_alarm('low', symbol)
            self.musical_alarm_system.play_note(symbol, self.muted_symbols)
            self.alarm_cooldowns[symbol] = current_time
            await self.update_debug_label(f"Low alarm triggered for {symbol}: {percent_gain:.2f}%")
            self.show_notification(f"Low Alert: {symbol} at {percent_gain:.2f}%", "warning", 2000)

    @profile_function
    def handle_chart_update(self, _):
        worker = Worker(self.prepare_chart_data)
        worker.signals.finished.connect(self.update_chart)
        self.threadpool.start(worker)

    @profile_function
    def prepare_chart_data(self):
        chart_data = {}
        now = make_timestamp_naive_utc(datetime.now(pytz.UTC))
        start_time = now - timedelta(seconds=self.chart_history_seconds)
        top_symbols = [symbol for _, symbol in self.top_tracker.get_top()]

        data_source = self.price_data if self.y_axis_mode == 'gain' else self.volume_price_data

        for symbol in top_symbols:
            if symbol in data_source:
                data = data_source[symbol]
                with data.lock:
                    valid_data = [(t, v) for t, v in data.buffer if t >= start_time]
                if valid_data:
                    last_timestamp, last_val = valid_data[-1]
                    if last_timestamp < now:
                        valid_data.append((now, last_val))
                    chart_data[symbol] = valid_data

        return chart_data

    @profile_function
    def update_chart(self, chart_data):
        logging.debug(f"[CHART UPDATE] Called update_chart with {len(chart_data)} symbols at {datetime.utcnow()}")
        has_data = False
        all_timestamps = []
        for symbol, data in chart_data.items():
            if data:
                has_data = True
                timestamps, values = zip(*data)
                timestamps = np.array([
                    t.replace(tzinfo=pytz.UTC).timestamp() if t.tzinfo is None
                    else t.astimezone(pytz.UTC).timestamp()
                    for t in timestamps
                ])
                values = np.array(values)
                all_timestamps.extend(timestamps)

                if symbol not in self.colors:
                    self.colors[symbol] = self.acquire_color()

                pen = pg.mkPen(color=self.colors[symbol], width=1.5)
                if symbol not in self.plot_curves:
                    curve = self.plot_widget.plot(
                        timestamps,
                        values,
                        pen=pen,
                        name=symbol,
                        stepMode='right'
                    )
                    self.plot_curves[symbol] = curve
                else:
                    curve = self.plot_curves[symbol]
                    curve.setData(timestamps, values, pen=pen, stepMode='right')

                # atomic read of latest price and map
                with self.data_lock:
                    idx = self.symbol_index_map.get(symbol)
                    latest_price = float(self.latest_prices[idx]) if idx is not None else float('nan')

                color_hex = self.colors.get(symbol, "#FFFFFF")
                label_html = (
                    f"<div style='color:{color_hex};background-color:rgba(0,0,0,180);"
                    f"padding:2px 4px;border-radius:3px'>{symbol}: "
                    f"${(latest_price if np.isfinite(latest_price) else 0.0):.2f}</div>"
                )
                if symbol not in self.text_items:
                    latest_time = timestamps[-1]
                    latest_gain = values[-1]
                    text_item = pg.TextItem(html=label_html, anchor=(0, 0.5))
                    text_item.setPos(latest_time, latest_gain)
                    self.plot_widget.addItem(text_item)
                    self.text_items[symbol] = text_item
                else:
                    latest_time = timestamps[-1]
                    latest_gain = values[-1]
                    text_item = self.text_items[symbol]
                    text_item.setHtml(label_html)
                    text_item.setPos(latest_time, latest_gain)

        symbols_to_remove = set(self.plot_curves.keys()) - set(chart_data.keys())
        for symbol in symbols_to_remove:
            curve = self.plot_curves.pop(symbol, None)
            if curve:
                self.plot_widget.removeItem(curve)
            text_item = self.text_items.pop(symbol, None)
            if text_item:
                self.plot_widget.removeItem(text_item)

        if not has_data:
            self.plot_widget.setTitle("Waiting for price data...")
        else:
            self.plot_widget.setTitle("Real-time Price Changes")

        if all_timestamps:
            min_time = min(all_timestamps)
            max_time = max(all_timestamps)
            range_time = max_time - min_time
            buffer_time = range_time * 0.25
            min_buffer = 1
            buffer_time = max(buffer_time, min_buffer)
            if self.chart_history_seconds == SHORT_CHART_HISTORY_SECONDS:
                view_width = self.plot_widget.width() or 1
                sec_per_px = SHORT_CHART_HISTORY_SECONDS / view_width
                dynamic_extra = max(
                    TEXT_LABEL_PADDING_PX * sec_per_px,
                    0.3 * SHORT_CHART_HISTORY_SECONDS,
                )
                buffer_time = max(buffer_time, dynamic_extra)
                self.plot_widget.setXRange(
                    max_time - SHORT_CHART_HISTORY_SECONDS,
                    max_time + buffer_time,
                    padding=0,
                )
            else:
                self.plot_widget.setXRange(min_time, max_time + buffer_time, padding=0)
            # Re-enable auto ranging on the Y axis so new data stays visible
            self.plot_widget.enableAutoRange(axis='y', enable=True)

        # UPDATED: Check against CHART_WARNING_THRESHOLD (499) instead of 99
        if not self.charts_warned and len(self.plot_curves) >= CHART_WARNING_THRESHOLD:
            self.charts_warned = True
            QTimer.singleShot(0, self.prompt_restart)

    @profile_function
    def prompt_restart(self):
        msg_box = QMessageBox(self)
        msg_box.setIcon(QMessageBox.Icon.Warning)
        msg_box.setWindowTitle("Chart Limit Reached")
        msg_box.setText(f"The chart area has reached {CHART_WARNING_THRESHOLD} charts. Do you want to restart the application?")
        msg_box.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        ret = msg_box.exec()

        if ret == QMessageBox.StandardButton.Yes:
            self.restart_application()
        else:
            self.charts_warned = False

    @profile_function
    def restart_application(self):
        logging.info("Restarting application as per user request.")
        QApplication.quit()
        python = sys.executable
        os.execl(python, python, *sys.argv)

    @profile_function
    async def chart_update_loop(self):
        while True:
            self.event_bus.publish(Event('CHART_UPDATE', None))
            await asyncio.sleep(CHART_UPDATE_INTERVAL)

    @profile_function
    async def update_loop(self):
        while True:
            self.update_top_tree_data()
            await asyncio.sleep(CHART_UPDATE_INTERVAL)

    @profile_function
    def update_top_tree_data(self):
        try:
            self.rate_limited_log('DEBUG', f"Updating top tree data. Current top symbols: {self.top_tracker.get_top()}")
            for symbol, score in self.top_tracker.symbol_scores.items():
                if symbol in self.symbol_index_map:
                    idx = self.symbol_index_map[symbol]
                    data = self.symbol_data[idx]
                    price_min = data['min']
                    price_max = data['max']
                    price_range = f"{price_min:.2f} - {price_max:.2f}" if price_min != price_max else f"{price_min:.2f}"
                    volume = data['volume']
                    base_price = self.base_prices[idx]
                    price_change = data['last'] - base_price
                    percent_change = (price_change / base_price) * 100 if base_price else 0
                    news_data = self.find_news_data(symbol)
                    headline = news_data['headline'] if news_data else 'No headline available'
                    self.top_tree_data[symbol] = {
                        'volume': volume,
                        'price_range': price_range,
                        'price_change': price_change,
                        'percent_change': percent_change,
                        'headline': headline
                    }
                else:
                    self.rate_limited_log('WARNING', f"Symbol {symbol} not found in symbol_index_map")
            QTimer.singleShot(0, self.update_top_tree_display)
            self.rate_limited_log('INFO', f"Top tree data updated. Symbols in top_tree_data: {list(self.top_tree_data.keys())}")
        except Exception as e:
            self.rate_limited_log('ERROR', f"Error in update_top_tree_data: {e}")
            self.rate_limited_log('ERROR', f"Current state: top_tracker={self.top_tracker}, symbol_index_map={self.symbol_index_map}")

    @profile_function
    def update_top_tree_display(self, force: bool = False):
        current_time = time.time()
        update_interval = 1.0
        if not force and current_time - self.last_top_tree_update < update_interval:
            return
        try:
            logging.debug(f"[TOP TREE DISPLAY] Clearing and populating top_tree at {datetime.utcnow()}")
            self.top_tree.blockSignals(True)
            self.top_tree.setUpdatesEnabled(False)
            self.top_tree.clear()
            self.top_tree_items.clear()

            top_symbols = [symbol for symbol_score, symbol in self.top_tracker.get_top()]
            row_height = max(self.top_tree.sizeHintForRow(0), 1)
            visible_count = self.top_tree.viewport().height() // row_height + 2
            for symbol in top_symbols[:visible_count]:
                if symbol in self.top_tree_data:
                    data = self.top_tree_data[symbol]
                    item = QTreeWidgetItem([
                        symbol,
                        str(data['volume']),
                        data['price_range'],
                        f"{data['price_change']:.2f} ({data['percent_change']:.2f}%)",
                        data['headline']
                    ])
                    item.setToolTip(4, data['headline'])
                    if symbol in self.colors:
                        color_hex = self.colors[symbol]
                        color = QColor(color_hex)
                        pixmap = QPixmap(16, 16)
                        pixmap.fill(color)
                        icon = QIcon(pixmap)
                        item.setIcon(0, icon)
                    if data['percent_change'] > 0:
                        item.setForeground(3, QColor('#10b981'))
                    elif data['percent_change'] < 0:
                        item.setForeground(3, QColor('#ef4444'))
                    else:
                        item.setForeground(3, QColor('#E0E0E0'))
                    self.top_tree.addTopLevelItem(item)
                    self.top_tree_items[symbol] = item

            self.top_tree.setUpdatesEnabled(True)
            self.top_tree.blockSignals(False)
            self.rate_limited_log('INFO', f"Top tree display updated. Item count: {self.top_tree.topLevelItemCount()}")
            self.last_top_tree_update = current_time
        except Exception as e:
            self.rate_limited_log('ERROR', f"Error in update_top_tree_display: {e}")

    @profile_function
    def find_news_data(self, symbol):
        if symbol in self.top_data and self.top_data[symbol]:
            return self.top_data[symbol][-1]
        base_symbol = symbol.split('.')[0]
        for full_symbol, news_list in self.top_data.items():
            if full_symbol.startswith(base_symbol) and news_list:
                return news_list[-1]
        return None

    @profile_function
    async def ensure_symbol_initialized(self, symbol: str):
        if symbol:
            if symbol not in self.colors:
                self.colors[symbol] = self.acquire_color()
            if symbol not in self.symbol_index_map:
                await self.fetch_last_trade(symbol)

    @profile_function
    def create_ticker_icon(self, symbols: List[str]) -> QIcon:
        """Create a small square icon showing one or more ticker colors."""
        count = max(1, min(len(symbols), 3))
        pixmap = QPixmap(16, 16)
        pixmap.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pixmap)
        segment_width = 16 // count
        for i in range(count):
            sym = symbols[i]
            color_hex = self.colors.get(sym, "#FFFFFF")
            painter.fillRect(i * segment_width, 0, segment_width, 16, QColor(color_hex))
        painter.end()
        return QIcon(pixmap)

    @profile_function
    def on_tree_click(self, item, button):
        if item:
            row_data = [item.text(i) for i in range(item.columnCount())]
            symbol = row_data[0] if row_data else None
            logging.info(f"[TREE CLICK] on_tree_click: symbol={symbol}, button={button}, time={datetime.utcnow()}")
            self.last_clicked_top_item = item
            self.selected_ric = symbol
            self.update_mute_button_state()
            asyncio.create_task(self.handle_selected_news(symbol))
            self.copy_to_clipboard_from_top(symbol)
            QTimer.singleShot(0, lambda symbol=symbol: self.update_recent_headlines_widget(symbol))
            if self.trading_mode_enabled and symbol:
                worker = Worker(self.perform_action_for_symbol_sync_wrapper, symbol, button)
                self.threadpool.start(worker)
                self.show_notification(f"Trading action initiated for {symbol}", "info", 2000)

    @profile_function
    def on_all_news_tree_click(self, item, button):
        if item:
            timestamp = item.text(0)
            source = item.text(1)
            ric = item.text(2)
            headline = item.text(3)
            logging.info(f"[ALL NEWS CLICK] timestamp={timestamp}, source={source}, ric={ric}, button={button}, time={datetime.utcnow()}")
            self.last_clicked_news_item = item

            # --- FIX: Reverse the priority ---
            head_rics = []

            # 1. Prioritize the dedicated RIC column (Column 2)
            if ric.strip('()'):
                ric_candidate_cleaned = self.clean_ric(ric.strip('()'))
                if ric_candidate_cleaned:
                    head_rics = ric_candidate_cleaned

            # 2. Fallback: If the RIC column didn't yield valid RICs, try the headline (less reliable)
            if not head_rics:
                match = re.search(r'\((.*?)\)', headline)
                if match:
                    candidate = match.group(1).strip()
                    candidate_cleaned = self.clean_ric(candidate)
                    if candidate_cleaned:
                        head_rics = candidate_cleaned

            if button == Qt.MouseButton.LeftButton:
                symbol = head_rics[0] if head_rics else None
            else:
                if len(head_rics) > 1:
                    symbol = head_rics[1]
                else:
                    # Fallback to the first RIC if only one exists, even on right-click
                    symbol = head_rics[0] if head_rics else None

            logging.info(f"[ALL NEWS CLICK] Extracted RICs: {head_rics}, chosen symbol: {symbol}")

            if symbol:
                self.selected_ric = symbol
                self.update_mute_button_state()
                asyncio.create_task(self.handle_selected_news(symbol))
                self.copy_to_clipboard_from_top(symbol)

                ric1 = head_rics[0] if len(head_rics) > 0 else "none"
                ric2 = head_rics[1] if len(head_rics) > 1 else "none"

                self.update_selected_news_display(
                    headline=headline,
                    timestamp=timestamp,
                    source=source,
                    ric1=ric1,
                    ric2=ric2,
                    ric3="none"
                )
                QTimer.singleShot(0, lambda symbol=symbol: self.update_recent_headlines_widget(symbol))

                if self.trading_mode_enabled and symbol:
                    worker = Worker(self.perform_action_for_symbol_sync_wrapper, symbol, button)
                    self.threadpool.start(worker)
                    self.show_notification(f"Trading action initiated for {symbol}", "info", 2000)
            else:
                self.debug_label.setText("No RICs match the filter criteria.")
                logging.warning(f"[ALL NEWS CLICK] No valid symbol found for item. RIC input={ric}")
                self.show_notification("No valid symbol found", "warning", 2000)

    @profile_function
    def perform_action_for_symbol_sync_wrapper(self, symbol, button):
        logging.info(f"[ACTION] perform_action_for_symbol_sync_wrapper: symbol={symbol}, button={button}, time={datetime.utcnow()}")

        current_price = None

        # FIX: Acquire the lock before reading shared data structures to prevent race conditions
        with self.data_lock:
            if symbol in self.symbol_index_map:
                idx = self.symbol_index_map[symbol]
                # Ensure consistency: the data at this index must match the requested symbol AND be initialized (not empty)
                # Accessing the NumPy structured array field.
                data_symbol = self.symbol_data[idx]['symbol']
                if data_symbol == symbol and data_symbol != '':
                    price = self.symbol_data[idx]['last']
                    # Basic validation that the stored price is positive (initialized)
                    if price > 0.0:
                       current_price = price
                else:
                    # Log if there is an inconsistency (e.g., index mismatch or data not yet populated)
                    logging.error(f"Data inconsistency or initialization pending for {symbol} at index {idx}. Found '{data_symbol}'.")

        # FIX: Remove fallback price. If price is unavailable or invalid, abort the action.
        if current_price is None:
            logging.warning(f"Unable to fetch valid price for {symbol}. Aborting action.")
            # UI updates must happen on the main thread (QTimer.singleShot required as this runs in a worker thread).
            QTimer.singleShot(0, lambda: self.show_notification(f"Price data not available for {symbol}. Please wait and try again.", "error", 3000))
            return

        logging.info(f"[ACTION] Symbol {symbol} current price: {current_price}")

        try:
            investment_amount = float(self.investment_amount_entry.text())
            slippage_percentage = float(self.slippage_percentage_entry.text())
        except ValueError:
            # FIX: Abort if inputs are invalid, rather than using default values.
            logging.warning("Invalid investment or slippage values. Aborting action.")
            # Ensure UI updates happen on main thread
            QTimer.singleShot(0, lambda: self.debug_label.setText("Invalid investment or slippage values. Aborting action."))
            QTimer.singleShot(0, lambda: self.show_notification("Invalid investment or slippage values. Aborting action.", "error", 3000))
            return

        quantity = calculate_quantity(current_price, investment_amount, slippage_percentage)

        # FIX: Remove fallback quantity. If calculation failed (e.g., price too low, investment too small), abort.
        if quantity is None or quantity <= 0:
            logging.warning(f"Calculated quantity is invalid ({quantity}) for {symbol}. Aborting action.")
            QTimer.singleShot(0, lambda: self.show_notification(f"Could not calculate valid quantity for {symbol}. Aborting.", "error", 3000))
            return

        logging.info(f"[ACTION] Final quantity for {symbol} is {quantity} shares.")
        take_action_sync(symbol, int(quantity), button)
        QTimer.singleShot(0, lambda: self.show_notification(f"Executed trade for {quantity} shares of {symbol}", "success", 3000))

    @profile_function
    def copy_to_clipboard_from_top(self, symbol):
        self.rate_limited_log('INFO', f"Attempting to copy to clipboard for {symbol} from top tree")
        try:
            clipboard_text = self.ric_to_clipboard_text.get(symbol)
            if clipboard_text:
                QApplication.clipboard().setText(clipboard_text)
                pattern = r'(\d{1,2} \w{3} - \d{2}:\d{2}:\d{2} [APM]{2})\s+\[(.*?)\]\s+\((.*?)\)\s+-\s+(.*)'
                match = re.match(pattern, clipboard_text)
                if match:
                    timestamp, source, r, headline = match.groups()
                    ric1 = r
                    ric2 = "none"
                    self.update_selected_news_display(
                        headline=headline,
                        timestamp=timestamp,
                        source=source,
                        ric1=ric1,
                        ric2=ric2,
                        ric3="none"
                    )
                    self.debug_label.setText(f"Copied to clipboard: {symbol}")
                    QTimer.singleShot(0, lambda symbol=symbol: self.update_recent_headlines_widget(symbol))
                    logging.info(f"Information for {symbol} copied to clipboard")
                else:
                    self.headline_label.setText(f"{clipboard_text}")
                    self.debug_label.setText(f"Copied to clipboard: {symbol} (Partial Data)")
                    logging.warning(f"Failed to parse clipboard_text for symbol {symbol}: {clipboard_text}")
            else:
                self.debug_label.setText(f"Error: No clipboard text found for {symbol}")
                logging.error(f"No clipboard text found for symbol {symbol}")
        except Exception as e:
            self.debug_label.setText(f"Error copying data for {symbol}: {str(e)}")
            logging.error(f"Exception in copy_to_clipboard_from_top for symbol {symbol}: {e}")

    @profile_function
    def copy_to_clipboard(self, symbol, timestamp, headline, source):
        self.rate_limited_log('INFO', f"Attempting to copy to clipboard for {symbol}")
        try:
            clipboard_text = self.generate_clipboard_text(timestamp, source, symbol, headline)
            QApplication.clipboard().setText(clipboard_text)
            self.headline_label.setText(f"{headline}")
            self.debug_label.setText(f"Copied to clipboard: {symbol}")
            self.ric_to_clipboard_text[symbol] = clipboard_text
            self.ric_clipboard_queue.append(symbol)
        except Exception as e:
            self.debug_label.setText(f"Error copying data for {symbol}: {str(e)}")
            logging.error(f"Error copying data for {symbol}: {e}")

    @profile_function
    def generate_clipboard_text(self, timestamp, source, symbol, headline):
        try:
            timestamp_obj = make_timestamp_naive_utc(timestamp)
            formatted_timestamp = timestamp_obj.strftime('%d %b - %I:%M:%S %p')
            source_text = f"[{source}]" if source else "[RTRS]"
            clean_source = source_text.strip('[]')
            clean_symbol = symbol.strip('()')
            return f"{formatted_timestamp}  [{clean_source}] ({clean_symbol}) - {headline}"
        except Exception as e:
            self.rate_limited_log('ERROR', f"Error generating clipboard text: {e}")
            return f"{timestamp} [{source}] ({symbol}) - {headline}"

    @profile_function
    def update_connection_status(self, connected):
        color = '#10b981' if connected else '#ef4444'
        self.connection_status.setStyleSheet(f"background-color: {color}; border-radius: 6px;")
        self.status_label.setText("Connected" if connected else "Disconnected")
        if connected:
            self.show_notification("Connected to server", "success", 2000)
        else:
            QTimer.singleShot(0, lambda: asyncio.create_task(self.play_disconnection_alarm()))
            self.show_notification("Disconnected from server", "error", 2000)

    @profile_function
    async def play_disconnection_alarm(self):
        for _ in range(3):
            for _ in range(3):
                self.audio_player.play_sound(880, 100)
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.5)

    @profile_function
    def clean_ric(self, ric_input):
        logging.info(f"[CLEAN RIC] clean_ric called with ric_input: {ric_input}")
        ric_input = ric_input.strip('()')
        rics = ric_input.split()
        cleaned_rics = []
        for ric in rics:
            ric = ric.strip('()')
            logging.info(f"[CLEAN RIC] Processing RIC: {ric}")
            match = re.match(r'(.+?)\.([A-Z]+)', ric)
            if match:
                symbol, exchange = match.groups()
                suffix = f'.{exchange}'
                if suffix not in ALLOWED_RIC_SUFFIXES:
                    self.rate_limited_log('INFO', f"RIC {ric} does not end with allowed suffix {ALLOWED_RIC_SUFFIXES}. Skipping.")
                    continue
                cleaned = re.sub(r'[a-z]', '', symbol)
                cleaned = re.sub(r'[^a-zA-Z0-9]', '', cleaned)
                if cleaned:
                    existing = self.symbol_mapping.get(cleaned, set())
                    if existing and ric not in existing:
                        logging.warning(
                            f"Collision detected for cleaned symbol {cleaned}: existing {existing}, new {ric}"
                        )
                        continue
                    self.symbol_mapping.setdefault(cleaned, set()).add(ric)
                    self.rate_limited_log('INFO', f"Cleaned RIC: {ric} to {cleaned}, mapped to {self.symbol_mapping[cleaned]}")
                    cleaned_rics.append(cleaned)
                else:
                    logging.warning(f"RIC {ric} cleaned symbol empty, skipping.")
            else:
                self.rate_limited_log('INFO', f"RIC {ric} does not match pattern. Skipping.")
                continue
        return cleaned_rics

    @profile_function
    def update_alarms(self, checked=False):
        try:
            self.hi_alarm = float(self.hi_alarm_entry.text())
            self.low_alarm = float(self.low_alarm_entry.text())
            self.debug_label.setText(f"Alarms updated: High {self.hi_alarm}%, Low {self.low_alarm}%")
            self.show_notification(f"Alarms updated: High {self.hi_alarm}%, Low {self.low_alarm}%", "success", 2000)
            for symbol, idx in self.symbol_index_map.items():
                current_price = self.symbol_data[idx]['last']
                base_price = self.base_prices[idx]
                if base_price > 0:
                    percent_gain = ((current_price - base_price) / base_price) * 100
                else:
                    percent_gain = 0
                self.loop.create_task(self.check_alarms(symbol, percent_gain))
        except ValueError:
            self.debug_label.setText("Invalid alarm values. Please enter numbers.")
            self.show_notification("Invalid alarm values", "error", 2000)

    @profile_function
    def play_alarm(self, alarm_type, symbol=None):
        if symbol and symbol in self.muted_symbols:
            return
        frequency = 960 if alarm_type == 'high' else 680
        duration = 100
        self.audio_player.play_sound(frequency, duration)

    @profile_function
    def rate_limited_log(self, level, message):
        current_time = time.time()
        if current_time - self.last_log_time >= self.log_interval:
            if level == 'INFO':
                logging.info(message)
            elif level == 'WARNING':
                logging.warning(message)
            elif level == 'ERROR':
                logging.error(message)
            elif level == 'DEBUG':
                logging.debug(message)
            self.last_log_time = current_time

    @profile_function
    async def shutdown(self):
        logging.info("Initiating shutdown...")
        await self.cleanup_websockets()
        if self.news_websocket and not self.news_websocket.closed:
            await self.news_websocket.close()
            self.rate_limited_log('INFO', "News WebSocket closed.")
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
        tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.threadpool.waitForDone()
        if hasattr(self, 'process_pool'):
            self.process_pool.shutdown(wait=True)
        if hasattr(self, 'shared_base'):
            self.shared_base.close()
            self.shared_base.unlink()
        if hasattr(self, 'shared_latest'):
            self.shared_latest.close()
            self.shared_latest.unlink()
        if hasattr(self, 'musical_alarm_system'):
            self.musical_alarm_system.stop()
        global queue_listener
        if queue_listener:
            queue_listener.stop()
        QApplication.quit()
        logging.info("Application shutting down")

    @profile_function
    async def update_debug_label(self, text):
        self.debug_label.setText(text)

    @profile_function
    def update_clock_label(self):
        if self.clock_mode != CLOCK_MODE_UTC:
            return
        ts_ns = time.time_ns()
        formatted = datetime.utcfromtimestamp(ts_ns // 1_000_000_000).strftime("%H:%M:%S.") + f"{ts_ns % 1_000_000_000:09d}"
        self.clock_label.setText(formatted)

    @profile_function
    def update_world_clock_label(self):
        if self.clock_mode == CLOCK_MODE_WORLD:
            now_utc = datetime.utcnow()
            cities = {
                "NY": "America/New_York",
                "LDN": "Europe/London",
                "IST": "Europe/Istanbul",
                "DXB": "Asia/Dubai",
                "KHI": "Asia/Karachi",
                "DEL": "Asia/Kolkata",
                "SHA": "Asia/Shanghai",
                "TYO": "Asia/Tokyo",
                "SYD": "Australia/Sydney",
            }
            parts = []
            utc = pytz.UTC.localize(now_utc)
            for abbr, zone in cities.items():
                local = utc.astimezone(pytz.timezone(zone))
                parts.append(f"{abbr} {local.strftime('%H:%M:%S.%f')[:-3]}")
            self.world_clock_label.setText(" | ".join(parts))
        elif self.clock_mode in (CLOCK_MODE_NY_12H, CLOCK_MODE_NY_24H):
            now_utc = datetime.utcnow()
            tz = pytz.timezone("America/New_York")
            local = pytz.UTC.localize(now_utc).astimezone(tz)
            if self.clock_mode == CLOCK_MODE_NY_12H:
                base = local.strftime("%I:%M:%S")
                suffix = local.strftime(" %p")
            else:
                base = local.strftime("%H:%M:%S")
                suffix = ""
            millis = int(local.microsecond / 1000)
            self.world_clock_label.setText(f"NY {base}.{millis:03d}{suffix}")

    @profile_function
    def cycle_clock_mode(self):
        modes = [CLOCK_MODE_WORLD, CLOCK_MODE_UTC, CLOCK_MODE_NY_12H, CLOCK_MODE_NY_24H]
        index = modes.index(self.clock_mode)
        new_index = (index + 1) % len(modes)
        self.on_clock_mode_changed(new_index)

    @profile_function
    def on_clock_mode_changed(self, index):
        modes = [CLOCK_MODE_WORLD, CLOCK_MODE_UTC, CLOCK_MODE_NY_12H, CLOCK_MODE_NY_24H]
        self.clock_mode = modes[index]
        if self.clock_mode == CLOCK_MODE_UTC:
            self.clock_label.show()
            self.world_clock_label.hide()
            self.clock_label.setStyleSheet("color: #BBBBBB; font-size: 16px;")
        else:
            self.clock_label.hide()
            self.world_clock_label.show()
            if self.clock_mode == CLOCK_MODE_WORLD:
                self.world_clock_label.setStyleSheet("color: #BBBBBB; font-size: 12px;")
            else:
                self.world_clock_label.setStyleSheet("color: #BBBBBB; font-size: 16px;")
        self.update_clock_label()
        self.update_world_clock_label()

    @profile_function
    def cleanup_old_data(self):
        now = make_timestamp_naive_utc(datetime.now(pytz.UTC))
        cutoff = now - timedelta(seconds=self.chart_history_seconds)
        for symbol in list(self.price_data.keys()):
            buffer = self.price_data[symbol]
            with buffer.lock:
                while buffer.buffer and buffer.buffer[0][0] < cutoff:
                    buffer.buffer.popleft()
        for symbol in list(self.volume_price_data.keys()):
            buffer = self.volume_price_data[symbol]
            with buffer.lock:
                while buffer.buffer and buffer.buffer[0][0] < cutoff:
                    buffer.buffer.popleft()

    @profile_function
    def cleanup_symbol_data(self):
        now = make_timestamp_naive_utc(datetime.now(pytz.UTC))
        now = now.replace(tzinfo=pytz.UTC).timestamp()
        cutoff = now - 6000
        with self.data_lock:
            for symbol, idx in list(self.symbol_index_map.items()):
                # Convert numpy datetime64 to Python datetime properly
                timestamp_ns = self.symbol_data[idx]['timestamp']
                if np.isnat(timestamp_ns):
                    continue  # Skip NaT (Not a Time) values
                
                # Convert from numpy datetime64 to Python datetime
                # datetime64[ns] stores nanoseconds since epoch
                timestamp_seconds = timestamp_ns.astype('datetime64[s]').astype(int)
                symbol_time_dt = datetime.utcfromtimestamp(timestamp_seconds)
                symbol_time = symbol_time_dt.replace(tzinfo=pytz.UTC).timestamp()
                
                if symbol_time < cutoff:
                    self.symbol_index_map.pop(symbol)
                    self.symbol_data[idx] = (
                        '', 0.0, 0.0, 0.0, 0, np.datetime64('NaT'), 0.0, 0.0
                    )
                    self.base_prices[idx] = 0.0
                    self.latest_prices[idx] = 0.0

                    # return slot to pool
                    self.available_indices.append(idx)

                    # purge per-symbol buffers
                    self.price_data.pop(symbol, None)
                    self.volume_price_data.pop(symbol, None)
                    self.volume_price_totals.pop(symbol, None)

                    # optional: remove visuals if present
                    if symbol in self.plot_curves:
                        self.plot_widget.removeItem(self.plot_curves.pop(symbol))
                    if symbol in self.text_items:
                        self.plot_widget.removeItem(self.text_items.pop(symbol))

        # assert post-cleanup
        self._assert_index_consistency()
                        
    @profile_function
    def cleanup_news_tree(self):
        while self.news_tree.topLevelItemCount() > 10000:
            self.news_tree.takeTopLevelItem(0)

    @profile_function
    def cleanup_ric_clipboard_text(self):
        while len(self.ric_to_clipboard_text) > 10000:
            oldest_ric = self.ric_clipboard_queue.popleft()
            self.ric_to_clipboard_text.pop(oldest_ric, None)

    @profile_function
    def cleanup_top_data(self):
        max_items_to_keep = 10000
        for ric, news_list in self.top_data.items():
            if len(news_list) > max_items_to_keep:
                self.top_data[ric] = news_list[-max_items_to_keep:]

    @profile_function
    async def cleanup_websockets(self):
        if self.polygon_websocket and not self.polygon_websocket.closed:
            await self.polygon_websocket.close()
            self.rate_limited_log('INFO', "Polygon WebSocket closed.")

    @profile_function
    async def perform_cleanup(self):
        while True:
            self.cleanup_old_data()
            self.cleanup_symbol_data()
            self.cleanup_news_tree()
            self.cleanup_ric_clipboard_text()
            self.top_tracker.cleanup_tracker(self.last_update_times)
            self.cleanup_top_data()
            await asyncio.sleep(3600)

    @profile_function
    def on_plot_click(self, event):
        pos = event.scenePos()
        if self.plot_widget.sceneBoundingRect().contains(pos):
            mouse_point = self.plot_widget.plotItem.vb.mapSceneToView(pos)
            click_x = mouse_point.x()
            click_y = mouse_point.y()
            x_tolerance = 2.0
            y_tolerance = 2.0

            closest_symbol = None
            min_distance = float('inf')

            for symbol, curve in self.plot_curves.items():
                x_data, y_data = curve.getData()
                if x_data is None or y_data is None:
                    continue
                delta_x = np.abs(x_data - click_x)
                delta_y = np.abs(y_data - click_y)
                within_tolerance = (delta_x <= x_tolerance) & (delta_y <= y_tolerance)
                if not np.any(within_tolerance):
                    continue
                distances = np.sqrt((x_data[within_tolerance] - click_x) ** 2 + (y_data[within_tolerance] - click_y) ** 2)
                local_min_distance = distances.min()
                if local_min_distance < min_distance:
                    min_distance = local_min_distance
                    closest_symbol = symbol

            overall_threshold = math.sqrt(x_tolerance ** 2 + y_tolerance ** 2)
        if closest_symbol and min_distance <= overall_threshold:
            self.copy_to_clipboard_from_top(closest_symbol)
            self.focus_symbol_on_chart(closest_symbol)

    @profile_function
    def on_plot_hover(self, pos):
        if self.plot_widget.sceneBoundingRect().contains(pos):
            mouse_point = self.plot_widget.plotItem.vb.mapSceneToView(pos)
            hover_x = mouse_point.x()
            hover_y = mouse_point.y()
            x_tolerance = 5.0
            y_tolerance = 5.0

            def nearest_distance(symbol):
                curve = self.plot_curves.get(symbol)
                if not curve:
                    return None
                x_data, y_data = curve.getData()
                if x_data is None or y_data is None:
                    return None
                delta_x = np.abs(x_data - hover_x)
                delta_y = np.abs(y_data - hover_y)
                within = (delta_x <= x_tolerance) & (delta_y <= y_tolerance)
                if not np.any(within):
                    return None
                distances = np.sqrt((x_data[within] - hover_x) ** 2 + (y_data[within] - hover_y) ** 2)
                return distances.min()

            overall_threshold = math.sqrt(x_tolerance ** 2 + y_tolerance ** 2)

            current_symbol = self.last_hovered_symbol
            current_distance = nearest_distance(current_symbol) if current_symbol else None

            closest_symbol = current_symbol
            min_distance = current_distance if current_distance is not None else float("inf")

            for symbol in self.plot_curves:
                dist = nearest_distance(symbol)
                if dist is not None and dist < min_distance:
                    min_distance = dist
                    closest_symbol = symbol

            if closest_symbol and min_distance is not None and min_distance <= overall_threshold:
                if closest_symbol != self.last_hovered_symbol:
                    self.focus_symbol_on_chart(closest_symbol)
                    self.last_hovered_symbol = closest_symbol
            else:
                if self.last_hovered_symbol is not None:
                    self.reset_chart_highlight()
                    self.last_hovered_symbol = None
        else:
            if self.last_hovered_symbol is not None:
                self.reset_chart_highlight()
                self.last_hovered_symbol = None

    @profile_function
    def format_large_number(self, value):
        if not isinstance(value, (int, float)):
            return str(value)
        if value < 10000:
            return str(int(value))
        if 10000 <= value < 1_000_000:
            return f"{value/1000:.0f}K"
        if 1_000_000 <= value < 1_000_000_000:
            return f"{value/1_000_000:.1f} M"
        if 1_000_000_000 <= value < 1_000_000_000_000:
            return f"{value/1_000_000_000:.1f} B"
        return f"{value/1_000_000_000_000:.1f} T"

    @profile_function
    async def get_polygon_data(self, ticker: str, date: str) -> Dict[str, Any]:
        ticker = ticker.upper().strip()
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?date={date}"
        headers = {"Authorization": f"Bearer {POLYGON_API_KEY}", "Accept": "application/json"}
        session = await self.ensure_http_session()
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == 'OK' and 'results' in data:
                        return data['results']
                    else:
                        self.rate_limited_log('WARNING', f"No company data found for {ticker}")
                        return {}
                elif response.status == 404:
                    self.rate_limited_log('ERROR', f"Ticker not found: {ticker}")
                    return {}
                else:
                    self.rate_limited_log('ERROR', f"Failed to fetch data for {ticker}: HTTP {response.status}")
                    return {}
        except aiohttp.ClientError as e:
            self.rate_limited_log('ERROR', f"Network error fetching data for {ticker}: {e}")
            return {}
        except Exception as e:
            self.rate_limited_log('ERROR', f"Unexpected error fetching data for {ticker}: {e}")
            return {}

    @profile_function
    def flush_ui_updates(self):
        logging.debug(f"[UI FLUSH] flush_ui_updates called at {datetime.utcnow()} - pending_news_items={len(self.pending_news_items)}")
        if self.pending_news_items:
            self.news_tree.setUpdatesEnabled(False)
            for QItem in self.pending_news_items:
                logging.debug(f"[UI INSERT] Inserting news item with headline: {QItem.text(3)}")
                self.news_tree.insertTopLevelItem(0, QItem)
            self.news_tree.setUpdatesEnabled(True)
            self.pending_news_items.clear()

    @profile_function
    def _assert_index_consistency(self):
        with self.data_lock:
            seen = {}
            for s, i in self.symbol_index_map.items():
                other = seen.get(i)
                if other is not None and other != s:
                    logging.critical(f"Index collision: {i} -> {other} and {s}")
                seen[i] = s

    def __del__(self):
        if getattr(self, 'polygon_websocket', None) and self.polygon_websocket and not self.polygon_websocket.closed:
            asyncio.create_task(self.polygon_websocket.close())
        if getattr(self, 'news_websocket', None) and self.news_websocket and not self.news_websocket.closed:
            asyncio.create_task(self.news_websocket.close())
        if getattr(self, 'http_session', None) and not self.http_session.closed:
            asyncio.create_task(self.http_session.close())

@profile_function
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--enable-uvloop", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()

    global ENABLE_PROFILING
    ENABLE_PROFILING = args.profile or os.getenv("ENABLE_PROFILING") == "1"

    verbose = args.verbose or os.getenv("VERBOSE") == "1"
    configure_logging(verbose)

    if args.enable_uvloop or os.getenv("UVLOOP") == "1":
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            logging.warning("uvloop requested but not installed")

    # Enable high DPI scaling if the attribute exists for the Qt version in use
    if hasattr(Qt, "AA_EnableHighDpiScaling"):
        # PyQt5 compatibility
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
    elif hasattr(Qt, "ApplicationAttribute") and hasattr(Qt.ApplicationAttribute, "AA_EnableHighDpiScaling"):
        # PyQt6
        QApplication.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling)

    # Use OpenGLES if available for GPU-accelerated painting
    if hasattr(Qt, "AA_UseOpenGLES"):
        QApplication.setAttribute(Qt.AA_UseOpenGLES)
    elif hasattr(Qt, "ApplicationAttribute") and hasattr(Qt.ApplicationAttribute, "AA_UseOpenGLES"):
        QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseOpenGLES)
    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    client = HFTInteractiveNewsClient()
    client.show()

    app.aboutToQuit.connect(lambda: asyncio.ensure_future(client.shutdown()))

    with loop:
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
