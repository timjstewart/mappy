import sys
from typing import Dict, Any

import mappy


class MyExceptionalMapper(mappy.Mapper):
    def _map(self, x: Dict[str, Any]) -> Dict[str, Any]:
        raise ValueError('An exception was raised.')


if __name__ == "__main__":
    mappy.process_files(MyExceptionalMapper(["a", "b"]), sys.argv[1:])
