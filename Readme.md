# Weather-API

This represents an API layer between weather forecast data and a front end.

This system can be run locally, but is meant to be run in a k8s cluster. This requires:
- A deployment with
    - resources: ~.5GB Memory and ~.25 CPU
    - envs: MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASS
- A secret with the above envs
- A service