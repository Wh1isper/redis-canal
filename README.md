![](https://img.shields.io/github/license/wh1isper/redis-canal)
![](https://img.shields.io/github/v/release/wh1isper/redis-canal)
![](https://img.shields.io/docker/image-size/wh1isper/redis-canal)
![](https://img.shields.io/pypi/dm/redis-canal)
![](https://img.shields.io/github/last-commit/wh1isper/redis-canal)
![](https://img.shields.io/pypi/pyversions/redis-canal)
[![codecov](https://codecov.io/gh/Wh1isper/redis-canal/graph/badge.svg?token=DI8L42sAMw)](https://codecov.io/gh/Wh1isper/redis-canal)

# redis-canal

Proxy redis stream from one to another through global queue service.

Motivation: Redis provided on the cloud is usually only available within a VPC and does not provide external access. We want to provide a way to synchronize messages across clouds without going through a VPN.

![Architecture Overview](./assets/Architecture.png)

## Supported queue service

- [x] AWS SQS

Welcome to contribute more queue service, see [adapter/impl](./redis_canal/adapter/impl/) for more details.

We also support the plugin system, the adapter can also be imported as a third-party library. See [example](./example/extension/custom-adapter/) for more details.

## Install

`pip install redis-canal[all]` for all components.

Or use docker image

`docker pull wh1isper/redis-canal`

## Usage

WIP

## Develop

Install pre-commit before commit

```
pip install pre-commit
pre-commit install
```

Install package locally

```
pip install -e .[test]
```

Run unit-test before PR

```
pytest -v tests
```
