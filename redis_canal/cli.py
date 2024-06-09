from pprint import pformat

import click


@click.command()
def stream_to_queue():
    pass


@click.command()
def queue_to_stream():
    pass


@click.group()
def cli():
    pass


cli.add_command(stream_to_queue)
cli.add_command(queue_to_stream)
