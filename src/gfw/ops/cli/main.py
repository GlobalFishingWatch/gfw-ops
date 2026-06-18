import sys
import logging

from ..version import __version__

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(args: list):
    logger.info("Starting APP (v{})...".format(__version__))


def main():  # Entry point for the python package.
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
