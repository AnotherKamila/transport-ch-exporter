#!/usr/bin/env python

import os
import yaml
import functools
from collections import namedtuple
from datetime import datetime
import time

import attr
import prometheus_client as prom
from prometheus_client.twisted import MetricsResource
from twisted.web import server, resource
from twisted.internet import defer, endpoints, reactor, task
import treq

METRICS_PREFIX   = 'transport'
API_BASEURL      = 'http://transport.opendata.ch/v1'
REFRESH_INTERVAL = 10*60  # seconds
CONNS_LIMIT      = 10     # how many connections we'll request in one go
MIN_TIME_LEFT    = 2*60   # seconds, before we consider this connection missed

# Yes yes, I am doing the exact opposite of
# https://prometheus.io/docs/practices/naming/#labels
# and putting IDs into labels.
# The idea is that one person or group of people does not care about all that
# many lines and thus it's fine.
# Use the optional `links_whitelist` config option if the number of
# metrics gets too large.
# ^^ UNIMPLEMENTED :D
ConnKey = namedtuple('ConnKey', ('station', 'category', 'number', 'to'))
NEXT_DEP = prom.Gauge('{}_next_departure'.format(METRICS_PREFIX),
                      'Time until next departure',
                      labelnames=ConnKey._fields,
                      unit='unixtime')

def ensure_deferred_f(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        result = f(*args, **kwargs)
        return defer.ensureDeferred(result)
    return wrapper


@attr.s
class Journey:
    """Wraps journey data returned from the transport API"""
    data = attr.ib()

    @property
    def key(self):
        """Raises IndexError when this is only walking!"""
        first_seg = self.get_sections()[0]
        return ConnKey(
            station= first_seg['departure']['station']['name'],
            category=first_seg['journey']['category'],
            number=  first_seg['journey']['number'],
            to=      first_seg['journey']['to'],
        )

    @property
    def departure(self):
        return datetime.fromtimestamp(self.departure_ts)

    @property
    def departure_ts(self):
        return self.get_sections()[0]['departure']['departureTimestamp']

    def get_sections(self, include_walk=False):
        if include_walk:
            return self.data['sections']
        else:
            return [s for s in self.data['sections'] if not s['walk']]


@attr.s
class TransportExporter:
    config     = attr.ib()
    conn_times = attr.ib(factory=dict)  # ConnKey => set(departure_ts)

    @ensure_deferred_f
    async def load_data(self, reactor):
        requests = []
        for conn in self.config['connections']:
            requests.append(self.load_conns(reactor, conn['from'], conn['to']))
        await defer.DeferredList(requests)

    @ensure_deferred_f
    async def load_conns(self, reactor, station_from, station_to):
        print('Looking up connnection: {} -> {}'.format(station_from, station_to))
        res  = await treq.get(API_BASEURL+'/connections', params={
            'from':     station_from,
            'to':       station_to,
            'limit':    CONNS_LIMIT,
            'fields[]': 'connections',
        })
        data = await res.json()
        for entry in data['connections']:
            j = Journey(entry)
            if not j.get_sections():  # skip walking-only
                print('WARNING: Got a walk-only journey, is that expected?')
                continue
            print('Got journey: {} at {}'.format(j.key, j.departure))

            if not j.key in self.conn_times:  # we have not seen this connection yet
                self.conn_times[j.key] = set()
                self.setup_metrics(j.key)
            self.conn_times[j.key].add(j.departure_ts)

    def get_next_departure(self, key):
        last_good = time.time() + MIN_TIME_LEFT
        self.conn_times[key] = set(t for t in self.conn_times[key] if t > last_good)
        return min(self.conn_times[key])

    def setup_metrics(self, key):
        NEXT_DEP.labels(*key).set_function(lambda: self.get_next_departure(key))


    def start_http_server(self, reactor):
        root = resource.Resource()
        root.putChild(b'metrics', MetricsResource())

        port = os.getenv('PORT', 8000)
        endpoints.serverFromString(
            reactor, r'tcp:interface=\:\:0:port={}'.format(port)
        ).listen(server.Site(root))
        print('HTTP server listening on port {}'.format(port))

    def start_refresh_loop(self, reactor):
        self.refresh_loop = task.LoopingCall(self.load_data, reactor)
        self.refresh_loop.start(REFRESH_INTERVAL)

    def start(self, reactor):
        self.start_refresh_loop(reactor)
        self.start_http_server(reactor)


def main():
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)

    transport_exporter = TransportExporter(config)
    transport_exporter.start(reactor)
    reactor.run()

if __name__ == '__main__':
    main()
