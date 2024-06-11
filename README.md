![](https://img.shields.io/github/license/wh1isper/redis-canal)
![](https://img.shields.io/github/v/release/wh1isper/redis-canal)
![](https://img.shields.io/docker/image-size/wh1isper/redis-canal)
![](https://img.shields.io/pypi/dm/redis-canal)
![](https://img.shields.io/github/last-commit/wh1isper/redis-canal)
![](https://img.shields.io/pypi/pyversions/redis-canal)
[![codecov](https://codecov.io/gh/Wh1isper/redis-canal/graph/badge.svg?token=DI8L42sAMw)](https://codecov.io/gh/Wh1isper/redis-canal)

# redis-canal

Proxy redis stream from one to another through global queue service

## Supported queue service

- [x] AWS SQS

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
