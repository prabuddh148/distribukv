---
title: DistribuKV
emoji: 🗄️
colorFrom: green
colorTo: indigo
sdk: docker
app_port: 7860
pinned: false
short_description: Dynamo-style distributed KV store with Kafka, MySQL, Redis
---

# DistribuKV · live cluster

A real 5-node DistribuKV cluster with Kafka (replication log), MySQL (per-node storage) and Redis (failure detection), running in this Space.

Open the app to use the dashboard: write keys, partition replicas and watch quorums, hinted handoff and the Kafka log at work. Partitions heal automatically after 60 seconds, and data resets when the Space restarts.

- Project page: https://prabuddh148.github.io/distribukv/
- Source code: https://github.com/prabuddh148/distribukv
