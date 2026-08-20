#! /usr/bin/env python3
"""
gpsd gateway
        
Configurable gateway to send gpsd TPV (Time Position Velocity) data to different endpoints: a GPSLogger-compatible endpoint, or a GPX file upload endpoint

Threading model:
* main thread starts all other threads up, handles shutdown signal processing and on demand tells threads to stop, waits for threads to complete, then returns
* stats thread that receives stats from all threads and periodically logs those stats if enabled
* reader thread connects to gpsd, reads all sentences from gpsd as fast as it sends them (one per second, otherwise it will fall behind), and enqueues all unique TPV (time position velocity) messages onto the sampling queue as points
* sampler thread reads the latest points from the sampling queue at a configured interval to reduce the data rate (defaults to once per 15 seconds, so about 1 out of 15 updates) while emptying the sampling queue, and puts the points into the send queue or if in batch mode into the batcher queue instead
* batcher thread waits for a batching interval (default 0 aka batching is disabled), then reads all points in batcher queue, generates a GPX file in-memory of all the points, and puts the GPX file into the send queue
* writer threads (default 1) wait for outputs-to-send in the sending queue, and sends any outputs to the configured endpoint immediately (and retries any failures)
"""

# built-in dependencies
import time
import sys
import signal
import time
import logging
import queue
import threading
import math
import json
import copy
import re
import shlex
from datetime import datetime
from dataclasses import dataclass
from unittest.mock import MagicMock, patch
from collections import defaultdict
from urllib.parse import urlparse

# external dependencies
import requests
import configargparse
import gps
import gpxpy

#########################################################################
# Version check
#########################################################################

# verify code is running under at least min supported Python version
MIN_PYTHON = (3, 12)
if sys.version_info < MIN_PYTHON:
    sys.exit("Python %s.%s or later is required.\n" % MIN_PYTHON)


#########################################################################
# Misc
#########################################################################

# create a namespaced logger for the module
# so if module is reused, the future user can configure logging for just this module
logger = logging.getLogger(__name__)


#########################################################################
# Signal processing
#########################################################################

# Interrupt key exception
class GracefulExit(BaseException):
    pass

def handle_sigterm(signum, frame) -> GracefulExit:
    """Handle SIGTERM: raise a GracefulExit exception via a function (for a signal handler)"""
    raise GracefulExit(f"Received signal {signum}")

def handle_sighup(signum, frame) -> None:
    """Handle SIGHUP: do nothing"""
    logger.info("SIGHUP received - nothing to do")


#########################################################################
# Stats tracking
#########################################################################

class Stats:
    """
    Track stats via key and value, in a thread-safe manner, both total and since last report
    """

    def __init__(self, interval_seconds=0):
        self._lock = threading.Lock()

        self._totalStats = defaultdict(int)
        self._recentStats = defaultdict(int)

        self._totalDateTime = datetime.now()
        self._recentDateTime = self._totalDateTime

    def increment(self, metric_name, value=1):
        """Thread-safe increment of a counter."""
        with self._lock:
            self._recentStats[metric_name] += value
            self._totalStats[metric_name] += value

    def set(self, metric_name, value=0):
        """Thread-safe update of a specific value."""
        with self._lock:
            self._recentStats[metric_name] = value
            self._totalStats[metric_name] = value

    def pptd(td: timedelta) -> str:
        """Pretty print a timedelta in a compact day hours minutes seconds format"""

        # Extract days
        days = td.days

        # Extract hours, minutes, and seconds from total seconds
        total_seconds = int(td.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Adjust hours if you want them wrapped per day (0-23)
        hours_per_day = hours % 24

        dstr = ""

        if days != 0:
            dstr = f"{days}d{hours_per_day:d}h{minutes:d}m{seconds:d}s"
        elif hours_per_day != 0:
            dstr = f"{hours_per_day:d}h{minutes:d}m{seconds:d}s"
        elif minutes != 0:
            dstr = f"{minutes:d}m{seconds:d}s"
        else:
            dstr = f"{seconds:d}s"

        return dstr

    def report(self):
        """Logs the total and recent stats and resets the recent stats"""
        with self._lock:
            # Create a shallow copy to log without holding the lock too long
            recentStatsSnapshot = self._recentStats.copy()
            totalStatsSnapshot = self._totalStats.copy()
            # calculate time diffs
            nowDateTime = datetime.now()
            recentTimeDiff = nowDateTime - self._recentDateTime
            totalTimeDiff = nowDateTime - self._totalDateTime
            # clear recent stats
            self._recentStats.clear()
            self._recentDateTime = nowDateTime

        logger.info(f"Total stats last {Stats.pptd(totalTimeDiff)}: {json.dumps(totalStatsSnapshot, sort_keys=True)}")
        logger.info(f"Recent stats last {Stats.pptd(recentTimeDiff)}: {json.dumps(recentStatsSnapshot, sort_keys=True)}")


#########################################################################
# Gateway
#########################################################################

@dataclass
class Point:
    """Abstract location sample that can be used to build payloads"""

    def __init__(self):
        self.latitude = None
        self.longitude = None
        self.elevation = None
        self.time = None
        self.timestamp = None
        self.accuracy = None
        self.velocity = None

        self.hdop = None
        self.vdop = None
        self.pdop = None
        self.timestring = None
        self.satellites = None
        self.filename = None
        self.direction = None
        self.type_of_gps_fix = None

    def isGoodEnough(self) -> bool:
        """
        Does the point have all data required of gpslogger endpoint?

        Returns:
            bool: if point is complete enough to be accepted by gpslogger endpoint
        """
        return all(arg is not None for arg in (self.latitude, self.longitude, self.elevation, self.timestamp, self.accuracy, self.velocity))


class GpsdGateway:
    """
    Gateway to read GPS data from gpsd and forward it to an endpoint
    """

    # type hint shortcuts
    type HeaderDict = dict[str, str]

    authHeadersShortcut = ['X-API-TOKEN', 'Authorization']
    """HTTP headers to add as a helper if a auth token is supplied on command line"""

    def __init__(self):
        self._lastTimestamp: int = 0
        """int: Unix timestamp of last data from gpsd session that was enqueued for sampling and potential sending"""
        self._stats = Stats()
        """Stats: stats for this gateway"""
        self._samplingQueue: queue.Queue = queue.Queue()
        """The queue that all fully-valid-for-sending points are enqueued to for sampling (only send 1 every X seconds)"""
        self._batchQueue: queue.Queue = queue.Queue()
        """The queue of all points waiting to be sent together in a batch, when batching is enabled"""
        self._sendQueue: queue.Queue = queue.Queue()
        """The queue containing outputs to send"""

        self._pointtemplate = None # see default in command line argparse
        """str: template used to create payload to send to point endpoint"""
        self._pointpattern = None
        """re.Pattern: regex pattern built from  _pointtemplate keys for substituion, cached on first evaluation"""

        self.isDryRun = False
        """Don't write/send if doing a dry run"""


    #########################################################################
    # Misc
    #########################################################################

    def checkStatsReport(self, interval: int = 0) -> None:
        """Report stats if interval != 0"""
        if (interval != 0):
            self._stats.report()

    def handleSighup(self, signum, frame) -> None:
        logger.info("SIGHUP received - forcing stats report")
        self._stats.report()


    #########################################################################
    # Data processing
    #########################################################################

    def processData(session: gps.gps.gps) -> Point:
        """
        Given a gpsd session object, process it and return an equivalent generic Point usable to build payloads for different endpoints

        Arguments:
            session (gps.gps.gps): gpsd library session

        Returns:
            Point: sampled point
        """
        logString = 'GPSd sent Mode: %s(%d)' % (("Invalid", "NO_FIX", "2D", "3D")[session.fix.mode], session.fix.mode)

        point = Point()

        # Check if latitude/longitude errors are available
        if (gps.isfinite(session.fix.epx) and gps.isfinite(session.fix.epy)):
            lat_err = session.fix.epy  # Latitude error in meters
            lon_err = session.fix.epx  # Longitude error in meters
            # Combine lat ang lon error to get total horizontal error margin
            point.accuracy = math.hypot(lat_err, lon_err)
            logString += f" Acc: {point.accuracy:.2f}m LatE {lat_err:.2f}m LonE {lon_err:.2f}m"

        # Get altitude
        # gpslogger expects HAE by default unless MSL turned ON which can result in missing alt
        # See https://github.com/mendhak/gpslogger/issues/748
        if gps.isfinite(session.fix.altHAE):
            point.elevation = session.fix.altHAE
            logString += f" alt: {point.elevation}m"
     
        # Get lat and lon
        if ((gps.isfinite(session.fix.latitude) and gps.isfinite(session.fix.longitude))):
            point.latitude = session.fix.latitude
            point.longitude = session.fix.longitude
            logString += " Lat %.6f Lon %.6f" % (point.latitude, point.longitude)

        # Get timestamp of gpsd data
        if gps.TIME_SET & session.valid:
            # Replace 'Z' with '+00:00' for ISO compliance and parse it
            # Format: "2026-08-05T20:08:00.000Z"
            clean_time = session.fix.time.replace('Z', '+00:00')
            point.time = datetime.fromisoformat(clean_time)
            point.timestring=session.fix.time

            timeStamp = gps.isotime(session.fix.time)
            point.timestamp = int(timeStamp)
            logString += ' Time: %s (%d)' % (point.time, point.timestamp)
   
        # Get speed
        if gps.isfinite(session.fix.speed):
            point.velocity = session.fix.speed # maybe * 3.6, # Convert m/s to km/h

        # TODO validate
        point.direction = session.fix.track
        point.hdop = session.hdop
        point.vdop = session.vdop
        point.pdop = session.pdop
        point.satellites = len(session.satellites)
        point.filename = session.input_file_name
        point.type_of_gps_fix = (None, "none", "2d", "3d")[session.fix.mode]
      
        logString  += ' END'
        logger.debug(logString)

        return point


    def checkData(self, point: Point) -> bool:
        """
        Does the point meet all requirements for being enqueued for sampling then sending?

        Arguments:
            point (Point): the potential point

        Returns:
            bool: should point be queued for sending to endpoint
        """
        if point.isGoodEnough() == False:
            self._stats.increment("payloadsIncomplete")
            return False

        if (self._lastTimestamp == int(point.timestamp)):
            logger.debug(f"Skipped enqueing payload with repeat timestamp {self._lastTimestamp}")
            self._stats.increment("payloadsRepeated")
            return False
        return True


    def enqueueData(self, point: Point) -> None:
        """
        Enqueue a received point for potential sampling and sending

        Arguments:
            point (Point): the point to enqueue
        """
        logger.debug(f"Enqued point {vars(point)}")
        self._samplingQueue.put(point)
        self._stats.increment("payloadsEnqueued")


    def buildGPSLoggerEndpointPayload(self, point: Point) -> str:
        """
        Build a gpslogger compatible payload for a Point

        Arguments:
            point (Point): the point to build a payload string for

        Returns:
            str: payload string representing point
        """
        # TODO support replacements in URL in addition to payload
        # see https://github.com/mendhak/gpslogger/blob/master/assets/text/faq/faq16-custom-url.md

        # gpslogger text block formatting:
        # https://github.com/mendhak/gpslogger/blob/master/gpslogger/src/main/java/com/mendhak/gpslogger/senders/customurl/CustomUrlManager.java#L176

        # battery level may be optional:
        # https://github.com/mendhak/gpslogger/blob/master/gpslogger/src/main/java/com/mendhak/gpslogger/common/SerializableLocation.java#L86
        # https://github.com/mendhak/gpslogger/blob/master/gpslogger/src/main/java/com/mendhak/gpslogger/loggers/customurl/CustomUrlLogger.java#L51

        # fields to possibly replace in the template
        replacements = {
            '%LAT': str(point.latitude),
            '%LON': str(point.longitude),
            '%ACC': str(point.accuracy),
            '%ALT': str(point.elevation),
            '%DIR': str(point.direction),
            '%TIMESTAMP': str(int(point.timestamp)),
            '%TIME': point.timestring,
            '%SPD': str(point.velocity), # m/s
            '%SPD_KPH': str(point.velocity * 3.6), # convert m/s to k/h
            '%BATT': "100",
            '%ISCHARGING': "true",
            '%HDOP': str(point.hdop),
            '%VDOP': str(point.vdop),
            '%PDOP': str(point.pdop),
            '%SAT': str(point.satellites),
            '%FILENAME': str(point.filename)
            }

        # TODO if possible
        # see https://github.com/mendhak/gpslogger/blob/master/gpslogger/src/main/java/com/mendhak/gpslogger/senders/customurl/CustomUrlManager.java#L191-L236
        # 't': "u" or "p"?
        # %DESC annotation
        # %PROV provider
        # %TIMEOFFSET time offset
        # %DATE date
        # %STARTTIMESTAMP epoch
        # %AID Android id
        # %SER serial
        # %PROFILE profile
        # %DIST travelled
        # %ALL all parameters

        # a few more possiblities from OwnTrack
        # see https://owntracks.org/booklet/tech/json/

        # compile pattern re first time only, for efficiency
        if self._pointpattern is None:
            # 1. Escape keys to handle special characters, then join them with an OR (|) pipe
            # TODO assignment not thread-safe kosher if multiple writer threads are running
            self._pointpattern = re.compile("|".join(re.escape(key) for key in replacements.keys()))

        # 2. Use a lambda function to look up the matched text in your dictionary
        payload = self._pointpattern.sub(lambda match: replacements[match.group(0)], self._pointtemplate)

        return payload


    def sendDataToGPSLoggerEndpoint(self, id: int, url: str, headers: HeaderDict, point: Point) -> bool:
        payload = self.buildGPSLoggerEndpointPayload(point)

        return self.sendData(id, url, headers, payload, False)


    def getLogString(payload) -> str:
        """
        Get a nice string to output to logs for a payload of string, dict, or __dict__-containing object""

        Returns:
            str: output string
        """
        
        if isinstance(payload, str):
            return payload
        elif isinstance(payload, dict):
            return payload
        else:
            return vars(payload)


    def sendData(self, id: int, url: str, headers: HeaderDict, payload: str, sendAsFile: bool) -> bool:
        """
        Send a str payload to the specified url with http headers as a POST of a Multipart-Encoded File

        Arguments:
            id (int): thread id number
            url (str): URL of endpoint to send data to
            headers (HeaderDict): http headers to use in http post
            payload (str): payload 
            sendAsFile (bool): Use HTTP POST multipart file instead of basic HTTP POST

        Returns:
            bool: was payload sent to endpoint
        """
        success = False
        try:
            # deep copy headers so we can add more headers if needed
            send_headers = copy.deepcopy(headers)

            logString = GpsdGateway.getLogString(payload)
            logger.debug(f"Writer {id} Sending to {url} asFile:{sendAsFile} headers {send_headers} data {logString}")

            # gpslogger endpoint receives variables as json but validates auth via headers
            response = None

            # make a fake response for testing
            if (self.isDryRun):
                response = MagicMock()
                response.status_code = 200
                response.ok = True
                response.json.return_value = {"status": "success"}
            # send the payload as a gpx file
            elif (sendAsFile):
                files = {'file': ('points.gpx', payload, 'application/gpx+xml')}
                # don't set Content-Type header on multipart file POST or it will break
                response = requests.post(url,  headers=send_headers, files=files, timeout=5)
            # send the payload as a point
            else:
                send_headers['Content-Type'] = 'application/json'
                response = requests.post(url,  headers=send_headers, data=payload, timeout=5)

            status_code = response.status_code
            response_string = response.text # in some instance response.json() might be better
            logger.debug(f"Writer {id} Status code {status_code} Response: {response_string}")

            if status_code == 200:
                success = True
            elif status_code in (401, 403):
                logger.error(f"Writer {id} Authentication failed! Check your auth token. Status code: {status_code}")
            else:
                logger.error(f"Writer {id} Endpoint rejected data. Status code: {status_code}")
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Writer {id} Network error forwarding to endpoint: {req_err}")

        return success

    #########################################################################
    # Thread entry methods
    #########################################################################

    def stats(self, id: int, stop_event: threading.Event, interval: int) -> None:
        """
        Thread of id to perodically log recent and total stats

        Arguments:
            id (int): thread id number
            stop_event (threading.Event): use to signal thread to stop and return
            interval (int): seconds between logging stats
        """

        logger.debug(f"Stats {id} starting")

        # even if interval is 0 to disable stats, go ahead and keep thread around (future reconfig on sighup)
        sleepinterval = interval if interval != 0 else None

        while not stop_event.is_set():
            interrupted = stop_event.wait(timeout=sleepinterval) 
            if (not interrupted):
                self.checkStatsReport(interval)

        logger.debug(f"Stats {id} ending")


    def reader(self, id: int, stop_event: threading.Event, url: str) -> None:
        """
        Thread of id to continuously read data from gpsd on server:port and enqueue any valid points for sampling

        Arguments:
            id (int): thread id number
            stop_event (threading.Event): use to signal thread to stop and return
            url (str): server to connect to
        """
        logger.debug(f"Reader {id} starting for {url}")

        # parse url
        parsed = urlparse(url)
        scheme = parsed.scheme
        server = parsed.hostname
        # use default port if not specified
        port = parsed.port or 2947

        if scheme != "gpsd":
            raise ValueError(f"Unknown or unsupported URL scheme: '{scheme}'")
            return

        while not stop_event.is_set():
            session = None

            try:
                logger.info(f"Connecting to gpsd at: tcp://{server}:{port}")
                session = gps.gps(host=server, port=port, mode=gps.WATCH_ENABLE) # TODO mode=gps.WATCH_ENABLE | gps.WATCH_NEWSTYLE
                logger.info(f"Connected to gpsd at: tcp://{server}:{port}")
                self._stats.increment("gpsdConnects")

                while (not stop_event.is_set()) and (0 == session.read()):
                    if not (gps.MODE_SET & session.valid):
                        # not useful, probably not a TPV message
                        self._stats.increment("gpsdMessagesNA")
                        continue
   
                    point = GpsdGateway.processData(session)
                    if self.checkData(point):
                        self._lastTimestamp = int(point.timestamp)
                        self.enqueueData(point)

            # These exceptions just cause a reconnection attempt
            except ConnectionRefusedError as e:
                logger.error(f"Could not connect to gpsd at {server}:{port}: {e}")
                self._stats.increment("gpsdConnectFailed")
                stop_event.wait(timeout=1) 
            # NOTE comment out to get better stacktrack
            except Exception as e:
                logger.error(f"Unknown error: {e}")
                self._stats.increment("gpsdOtherErrors")
                stop_event.wait(timeout=1) 

        if session is not None: session.close()
        logger.debug(f"Reader {id} ending for {server}:{port}")


    def sampler(self, id: int, stop_event: threading.Event, interval: int, batching: bool) -> None:
        """
        Thread of id to perioidically at interval seconds to sample 1 payload from the samplingQueue and enqueue it on sendingQueue for sending or the batchQueue for batching

        Arguments:
            id (int): thread id number
            stop_event (threading.Event): use to signal thread to stop and return
            interval (int): seconds between sampling 1 payload from the samplingQueue
            batching (bool): enable batching, aka put sampled points on batchQueue instead of sendingQueue
        """
        logger.debug(f"Sampler {id} starting")

        while not stop_event.is_set():
            latest = None
            count = 0

            # pop all points off the samplingQueue
            while not stop_event.is_set():
                try:
                    latest = self._samplingQueue.get_nowait()
                    if latest is not None: count += 1
                    # interval of 0 means send every enqueued payload, so skip taking more than 1 payload from queue
                    if interval == 0: break
                except queue.Empty:
                    break

            # if the queue is empty, then block for first entry to avoid adding latency (and either just started or already waited min time between sends)
            if latest is None:
                logger.debug(f"Sampler {id} Queue empty so do blocking get (count {count})")
                while not stop_event.is_set():
                    try:
                        latest = self._samplingQueue.get(block=True, timeout=1)
                        if latest is not None:
                            count += 1
                            break
                    except queue.Empty:
                        pass

            # if there was a point, queue it for sending or batching
            if latest is not None:
                if batching:
                    self._batchQueue.put(latest)
                else:
                    self._sendQueue.put(latest)
                logger.debug(f"Sampler {id} Sampled 1 of {count} and queued to send, and sleeping for {interval}")
                self._stats.increment("samplerCount")
                self._stats.increment("samplerEvaled", count)
                stop_event.wait(timeout=interval)
            else:
                logger.debug(f"Sampler {id} Queue empty after blocking get (count {count})")

        logger.debug(f"Sampler {id} ending")


    def batcherGPX(self, id: int, stop_event: threading.Event, interval: int) -> None:
        """
        Thread of id to perioidically at interval seconds to take all points from the batchQueue, create GPX xml file, and enqueue it on sendingQueue for sending

        Arguments:
            id (int): thread id number
            stop_event (threading.Event): use to signal thread to stop and return
            interval (int): seconds between creating a GPX xml batch of all points on the batchQueue
        """
        logger.debug(f"BatcherGPX {id} starting")

        while (interval != 0) and (not stop_event.is_set()):
            # when wait ends, either have enough points to send or shutting down
            # this usually allows a last batch to be sent on shutdown (but not guarenteed)
            stop_event.wait(timeout=interval)

            # Create track points
            track_points = []

            # create batch from all points queued
            while True:
                try:
                    latest = self._batchQueue.get_nowait()
                    point = gpxpy.gpx.GPXTrackPoint(
                        latitude=latest.latitude,
                        longitude=latest.longitude,
                        elevation=latest.elevation,
                        speed=latest.velocity,
                        time=latest.time,
                        horizontal_dilution=latest.hdop,
                        vertical_dilution=latest.vdop,
                        position_dilution=latest.pdop
                        )
                    # GPX 1.0 fields?
                    point.course=latest.direction
                    point.type_of_gps_fix = latest.type_of_gps_fix

                    track_points.append(point)
                except queue.Empty:
                    break

            numPoints = len(track_points)

            # might get an empty array on shutdown, so skip if so
            if numPoints <= 0:
                logger.debug(f"BatcherGPX {id} {numPoints} to batch so skipping")
                continue

            self._stats.increment("batcherPoints", numPoints)
            logger.debug(f"BatcherGPX {id} GPX encoding batch of {numPoints}")
           
            # see https://pypi.org/project/gpxpy/ for gpx generation docs
            gpx = gpxpy.gpx.GPX()

            # Create first track in our GPX:
            gpx_track = gpxpy.gpx.GPXTrack()
            gpx.tracks.append(gpx_track)

            # Create first segment in our GPX track:
            gpx_segment = gpxpy.gpx.GPXTrackSegment()
            gpx_track.segments.append(gpx_segment)

            # Create points:
            for t in track_points:
                gpx_segment.points.append(t)

            # pass to batch uploader thread
            gpx_string = gpx.to_xml()

            # queue the GPX file for sending
            logger.debug(f"BatcherGPX {id} GPX queuing batch of {len(track_points)}")
            self._sendQueue.put(gpx_string)
            self._stats.increment("batcherBatches")

        logger.debug(f"Batcher {id} ending")


    def writer(self, id: int, stop_event: threading.Event, url: str, headers: HeaderDict, batching: bool) -> None:
        """
        Thread of id to continuously read points from sendingQueue and send point to endpoint at url with http headers

        Arguments:
            id (int): thread id number
            stop_event (threading.Event): use to signal thread to stop and return
            url (str): endpoint URL to http POST payload
            headers (HeaderDict): http headers to apply to POST
        """
        logger.debug(f"Writer {id} starting")

        latest = None

        while not stop_event.is_set():
            # get an output to send, if there is one
            try:
                latest = self._sendQueue.get(block=True, timeout=1)
            except queue.Empty:
                continue

            if latest is not None:
                int_timestamp = int(time.time())
                queueSize = self._sendQueue.qsize()
                logger.debug(f"Writer {id} Sending data (of {queueSize} @ {int_timestamp})")
                success = False

                # send gpx xml as a file if batching
                if batching:
                    success = self.sendData(id, url, headers, latest, True)
                # otherwise send a point
                else:
                    # TODO move encoding of points into string payload to previous step in pipeline in point mode, to mirror latest being XML string from previous step in batching mode
                    success = self.sendDataToGPSLoggerEndpoint(id, url, headers, latest)

                if not success:
                    self._stats.increment("sendsFailed")
                    logger.error(f"Writer {id} Failed sending data, re-enqueing @ {int_timestamp}")
                    # pause for a bit?
                    stop_event.wait(timeout=1) 
                    # if sending fails, toss it back on queue for later attempt?
                    # TODO: stop queue from eating all memory if endpoint down for a long time
                    self._sendQueue.put(latest)
                else:
                    self._stats.increment("sendsSuccess")
        
        logger.debug(f"Writer {id} ending")


    #########################################################################
    # Main loop
    #########################################################################

    def run(self, sourceurl:str, url: str, headers:HeaderDict, token:str, interval:int, batchinterval:int, numwriters:int, statsinterval: int, pointtemplate:str, dryrun:bool) -> None:
        """
        Run the gpsd-gateway

        Runs until unrecoverable error, KeyboardInterrupt, or kill signal

        Arguments:
            sourceurl (str): hostname and port of gpsd server
            url (str): gpslogger endpoint URL to http POST payload
            headers (HeaderDict): HTTP headers for http POST
            token (str): Shortcut to add auth token to HTTP headers
            interval (int): seconds between sampling 1 point from the samplingQueue
            batchinterval (int): seconds between sending all points in the batchQueue, or 0 if batching is disabled
            numwriters (int): number of writer threads to gpslogger endpoing to start
            statsinterval (int): number of seconds between stats reports, or 0 for none
        """
        logger.info(f"Targeting endpoint URL: {url}")

        ## Setup

        self.isDryRun = dryrun
        self._pointtemplate = pointtemplate

        httpheaders = headers

        # shortcut for common auth headers if auth token is set
        if (token is not None):
            for header in GpsdGateway.authHeadersShortcut:
                httpheaders[header] = token

        ## Threads
     
        # stop signal event to threads, to make KeyboardInterrupt and graceful SIGKILL work
        stop_event = threading.Event()

        # all threads being created and run
        threads = []

        try:
            # Create stats thread
            t = threading.Thread(target=self.stats, args=(0, stop_event, statsinterval))
            statsThread = t
            threads.append(t)

            # Create reader thread
            t = threading.Thread(target=self.reader, args=(0, stop_event, sourceurl))
            readerThread = t
            threads.append(t)

            # Create sampler thread
            t = threading.Thread(target=self.sampler, args=(0, stop_event, interval, batchinterval != 0))
            samplerThread = t
            threads.append(t)

            # Create batcher thread
            t = threading.Thread(target=self.batcherGPX, args=(0, stop_event, batchinterval))
            batcherGPXThread = t
            threads.append(t)

            # Create writer threads
            writerThreads = []
            for id in range(numwriters):
                t = threading.Thread(target=self.writer, args=(id, stop_event, url, headers, batchinterval != 0))
                writerThreads.append(t)
                threads.append(t)

            # run all threads
            for t in threads: t.start()

            # Keep the main thread alive and responsive to KeyboardInterrupt
            while any(t.is_alive() for t in threads):
               # A short timeout allows Ctrl+C to interrupt the sleep
               time.sleep(0.5)

        # These exceptions shut down the gateway
        except KeyboardInterrupt as e:
            logger.info(f"Shutting down due to Control-C {e}")
        except GracefulExit as e:
            logger.info(f"Shutting down due to kill signal: {e}")
        # NOTE comment out to get better stacktrack
        except Exception as e:
            logger.error(f"Unknown error: {e}")
        finally:
            logger.info(f"Signaling threads to stop and waiting...")

            # Signal all threads to drop out of their loops
            stop_event.set()

            # wait for main threads to join, but keep stats thread running
            for t in threads: t.join()

            # output one last stats report
            self.checkStatsReport(statsinterval)
 
            logger.info(f"Done")
 

#########################################################################
# Argument parsing
#########################################################################

class ParseKeyValue(configargparse.Action):
    """Custom argparse action to read key=value pairs into a dictionary"""
    def __call__(self, parser, namespace, values, option_string=None):
        # Initialize dictionary if it doesn't exist yet
        d = getattr(namespace, self.dest, None) or {}
        if not isinstance(values, list):
            values = [values]

        # HACK: flatten lists because of odd configargparse behavior where configfile "X = a=1 b=1 c=1" results in value [['a=1', 'b=2', 'c=3']]
        flat_list = [item for sublist in values for item in sublist]

        # split k=v strings
        for val in flat_list:
            if '=' in val:
                k, v = val.split('=', 1)
                d[k.strip()] = v.strip()

        setattr(namespace, self.dest, d)


def split_values(value):
    """Splits value into a list of space separated items, allowing quoted items or interior quotes"""
    retval = None
    if isinstance(value, list):
        # else already a list (e.g., if using YAML parser)
        retval = value
    else:
        if value.count('=') <= 1:
            # hack for command line arguments passed as value: "Test1=AAA BBB"
            # NOTE deal with nested equals like Test1=AAA=BBB?
            retval = [value]
        else:
            # Split by spaces, allowing quoting
            # Test1=AAA Test2="AAA BBB"
            retval = shlex.split(value)

    return retval


def parse_arguments() -> configargparse.Namespace:
    """
    Parse arguments

    Returns:
        configargparse.Namespace: parsed configuration
    """

    parser = configargparse.ArgumentParser(
        description="Configurable gateway to send gpsd TPV (Time Position Velocity) data to different endpoints",
        formatter_class=configargparse.ArgumentDefaultsHelpFormatter
        #config_file_parser_class=configargparse.YAMLConfigFileParser
        #config_file_parser_class=configargparse.ConfigparserConfigFileParser
    )
    parser.add_argument('-c', '--config', is_config_file=True, help='Path to config file')
    parser.add_argument('-n', '--dryrun', default=False, action='store_true', help='Perform a dry run with no data sent to endpoint')
    parser.add_argument('-s', '--sourceurl', metavar="URL", default="gpsd://localhost:2947", help="Address of gpsd daemon in gpsd:// URL format")
    parser.add_argument('-u', '--url', default="http://localhost:8080/api/v1/ingest/gpslogger", help="Endpoint URL")
    parser.add_argument('-x', '--header', metavar="X=Y", dest='headers', nargs='*', type=split_values, action=ParseKeyValue, default={}, help="Add HTTP header")
    parser.add_argument('-t', '--token', help="Authorization token passed in the HTTP header (automatically added to headers as X-API-TOKEN and Authorization")
    parser.add_argument('-i', '--interval', metavar="SECS", type=int, default=15, help="Time in seconds between sampling a point, 0 sends every point")
    parser.add_argument('-b', '--batchinterval', metavar="SECS", type=int, default=0, help="Time in seconds between sending point sample batches, 0 disables")
    parser.add_argument('-w', '--numwriters', metavar="NUM", type=int, default=1, help="Number of writer threads to send to payloads to endpoint")
    parser.add_argument('-l', '--loglevel', default='WARNING', type=str.upper, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set the logging level')
    parser.add_argument('-r', '--statsinterval', metavar="SECS", type=int, default=0, help="Interval time in seconds between stats reports to INFO log, 0 disables, SIGHUP forces report")
    parser.add_argument('-e', '--pointtemplate', type=str, default='''{"_type": "location", "t": "u", "batt": "%BATT", "bs": "%ISCHARGING", "acc": %ACC, "alt": %ALT, "lat": %LAT, "lon": %LON, "tst": %TIMESTAMP, "vel": %SPD}''', help="GPSLogger compatible template used for sending points in point mode")
    return parser.parse_args()

def args_get_filtered_log_string(args, filter_list):
  """Converts argparse Namespace to a log string, masking keys in filter_list."""
  args_dict = vars(args)
  filtered_items = []

  for key, value in args_dict.items():
    if key in filter_list:
      log_value = "***" if value is not None else "None"
    else:
      log_value = str(value)
    filtered_items.append(f"{key}={log_value}")

  return ", ".join(filtered_items)


#########################################################################
# Main
#########################################################################

def main():
    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGHUP, handle_sighup)

    args = parse_arguments()

    # Configure the logger
    logging.basicConfig(
        level=args.loglevel,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logger.info(f"Starting gpsd-gateway...")
    logger.info(f"Arguments: {args_get_filtered_log_string(args, ['token'])}")

    app = GpsdGateway()
    # switch SIGHUP to be handled by GpsdGateway object for forced stats reporting
    signal.signal(signal.SIGHUP, app.handleSighup)
    app.run(
        sourceurl=args.sourceurl,
        url=args.url,
        headers=args.headers,
        token=args.token,
        interval=args.interval,
        batchinterval=args.batchinterval,
        numwriters=args.numwriters,
        statsinterval=args.statsinterval,
        pointtemplate=args.pointtemplate,
        dryrun=args.dryrun
    )

if __name__ == "__main__":
    main()
