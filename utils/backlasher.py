#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# backslasher.py

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


def discretize(tws, twf, backlash):

    def _round(t):
        return t - (t % backlash)

    ntws = _round(tws)
    ntwf = _round(twf)

    return ntws, ntwf
