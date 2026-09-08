import logging
import sys

from tap_spreadsheets_anywhere import main

# Useful for debugging
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG, stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.debug("This message should appear on the console")
sys.exit(main())
