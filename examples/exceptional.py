import sys
from typing import Dict, Any

import parcsv


class MyExceptionalMapper(parcsv.Mapper):
    def _map(self, x: Dict[str, Any]) -> Dict[str, Any]:
        raise ValueError('An exception was raised.')


if __name__ == "__main__":
    parcsv.process_files(MyExceptionalMapper(["a", "b"]), sys.argv[1:])
