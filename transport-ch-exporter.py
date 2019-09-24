#!/usr/bin/env python

import os
import yaml
import functools
from collections import namedtuple
from datetime import datetime

import attr
import prometheus_client as prom
from prometheus_client.twisted import MetricsResource
from twisted.web import server, resource
from twisted.internet import defer, endpoints, reactor, task
import treq

METRICS_PREFIX   = 'transport'

# Yes yes, I am doing the exact opposite of
# https://prometheus.io/docs/practices/naming/#labels
# and putting IDs into labels.
# The idea is that one person or group of people does not care about all that
# many lines and thus it's fine.
# Use the optional `links_whitelist` config option if the number of
# metrics gets too large.
ConnKey = namedtuple('ConnKey', ('station', 'name', 'category', 'number'))
NEXT_DEP = prom.Gauge('{}_next_departure'.format(METRICS_PREFIX),
                      'Time until next departure',
                      ConnKey._fields,  # labels
                      # unit='unixtime')
                      unit='seconds')

def ensure_deferred_f(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        result = f(*args, **kwargs)
        return defer.ensureDeferred(result)
    return wrapper

# class TransportExporter:
REFRESH_INTERVAL = 10*60  # seconds

CONNECTIONS = []
# We bucket connections for the NEXT_DEPARTURE_IN thingy.
CONN_BUCKETS = {}  # ConnKey => data

def no_walk(sections):
    return [s for s in sections if not s['walk']]

def get_departure(conn):
    # return parsedate(no_walk(conn['sections'])[0]['departure']['departure'])
    return no_walk(conn['sections'])[0]['departure']['departureTimestamp']

# def remove_old_connections():
    # TODO implement
    # for c in CONNECTIONS:

# TODO you don't want duplicate conns either

def getkey(conn):
    first_seg = no_walk(conn['sections'])[0]
    return ConnKey(
        station= first_seg['departure']['station']['name'],
        name=    first_seg['journey']['name'],
        category=first_seg['journey']['category'],
        number=  first_seg['journey']['number'],
    )

def bucketise(conn):
    key = getkey(conn)
    CONN_BUCKETS.setdefault(key, [])
    CONN_BUCKETS[key].append(conn)

@ensure_deferred_f
async def get_data(reactor, config):
    URL  = 'http://transport.opendata.ch/v1/connections?from=Zürich,%20Maienstrasse&to=Zürich%20Haldenbach&limit=10&fields[]=connections'
    res  = await treq.get(URL)
    data = await res.json()
    CONNECTIONS.append(data['connections'])
    for c in data['connections']:
        print('Got conn: {} at {}'.format(getkey(c), datetime.fromtimestamp(get_departure(c))))
        bucketise(c)
    set_up_metrics()

def get_next_departure(key):
    return min(get_departure(c) for c in CONN_BUCKETS[key])

def set_up_metrics():
    for key, conns in CONN_BUCKETS.items():
        NEXT_DEP.labels(*key).set_function(lambda: int(get_next_departure(key)))

def start_http_server(reactor):
    root = resource.Resource()
    root.putChild(b'metrics', MetricsResource())

    port = os.getenv('PORT', 8000)
    endpoints.serverFromString(
        reactor, r'tcp:interface=\:\:0:port={}'.format(port)
    ).listen(server.Site(root))
    print('HTTP server listening on port {}'.format(port))

def main():
    # read config
    config = None
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)

    # set up metrics
    for station in config['stations']:
        print(station)

    refresh_loop = task.LoopingCall(get_data, reactor, config)
    refresh_loop.start(REFRESH_INTERVAL)

    start_http_server(reactor)
    reactor.run()

if __name__ == '__main__':
    main()
