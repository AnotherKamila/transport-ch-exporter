# transport-ch-exporter

![contributions welcome!](https://img.shields.io/badge/contributions-welcome!-brightgreen)

Prometheus exporter for the [transport.opendata.ch](https://transport.opendata.ch/) public transport API.

Haven't you always wanted to have a proper dashboard to check your next tram? ;-)

![grafana dashboard screenshot](./screenshot.png)

Add your connections to `config.yml` (see the example included here).

In your dashboards, use queries such as:
```
min(transport_next_departure_unixtime{number=~"61|62"} - time())
```
