import sys
from typing import Dict, Any

import mappy


class MyIdentityMapper(mappy.Mapper):
    def _map(self, x: Dict[str, Any]) -> Dict[str, Any]:
        return x


if __name__ == "__main__":
    fieldnames = [
        "fl_site_deductible",
        "statecode",
        "hu_site_deductible",
        "line",
        "point_granularity",
        "policyID",
        "fl_site_limit",
        "fr_site_limit",
        "point_longitude",
        "eq_site_limit",
        "hu_site_limit",
        "construction",
        "tiv_2012",
        "point_latitude",
        "county",
        "fr_site_deductible",
        "tiv_2011",
        "eq_site_deductible",
    ]
    mappy.process_files(MyIdentityMapper(fieldnames), sys.argv[1:])
