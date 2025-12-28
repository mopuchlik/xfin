from fin_forecast.io.parsers import MstBarParser
from fin_forecast.domain.models import OHLCVBar


def test_mst_parser_single_line():
    parser = MstBarParser()

    lines = [
        "06MAGNA,19970612,106.0000,106.0000,106.0000,106.0000,14793.0000"
    ]

    bars = list(parser.parse_lines(lines))

    assert len(bars) == 1
    bar = bars[0]

    assert isinstance(bar, OHLCVBar)
    assert bar.ticker == "06MAGNA"
    assert bar.dt.year == 1997
    assert bar.open == 106.0
    assert bar.close == 106.0
    assert bar.vol == 14793.0


def test_parser_ignores_empty_lines():
    parser = MstBarParser()

    lines = [
        "",
        "AAA,20240101,1,2,0.5,1.5,10",
        "",
    ]

    bars = list(parser.parse_lines(lines))
    assert len(bars) == 1
