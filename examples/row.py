import sys
from typing import Dict, Any

import parcsv


class MyMapper(parcsv.Mapper):
    def _map(self, x: Dict[str, Any]) -> Dict[str, Any]:
        return x


if __name__ == "__main__":
    parcsv.process_files(MyMapper(["row"]), sys.argv[1:])
