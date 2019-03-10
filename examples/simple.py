import sys
from typing import Dict, Any

import parcsv


class MyMapper(parcsv.Mapper):
    def _map(self, x: Dict[str, Any]) -> Dict[str, Any]:
        print("stdout output")
        sys.stderr.write("stderr output")
        return dict(a=1, b=2)


if __name__ == "__main__":
    parcsv.process_files(MyMapper(["a", "b"]), sys.argv[1:])
